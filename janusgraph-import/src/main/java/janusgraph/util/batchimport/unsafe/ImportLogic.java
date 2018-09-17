package janusgraph.util.batchimport.unsafe;


import janusgraph.util.batchimport.unsafe.graph.store.ImportStore;
import janusgraph.util.batchimport.unsafe.helps.Dependencies;
import janusgraph.util.batchimport.unsafe.helps.collection.PrimitiveLongIterator;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Input;
import janusgraph.util.batchimport.unsafe.io.fs.FileSystem;
import janusgraph.util.batchimport.unsafe.log.Log;
import janusgraph.util.batchimport.unsafe.log.LogService;
import janusgraph.util.batchimport.unsafe.stage.*;
import janusgraph.util.batchimport.unsafe.stats.DataStatistics;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static janusgraph.util.batchimport.unsafe.helps.ByteUnit.bytes;
import static janusgraph.util.batchimport.unsafe.helps.Format.duration;
import static janusgraph.util.batchimport.unsafe.stage.ExecutionSupervisors.superviseExecution;
import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

/**
 * Contains all algorithms and logic for doing an import. It exposes all stages as methods so that
 * it's possible to implement a {@link BatchImporter} which calls those.
 * This class has state which typically gets modified in each invocation of an import method.
 *
 * To begin with the methods are fairly coarse-grained, but can and probably will be split up into smaller parts
 * to allow external implementors have greater control over the flow.
 */
public class ImportLogic implements Closeable
{
    public interface Monitor
    {
        void doubleEdgeRecordUnitsEnabled();

        void mayExceedNodeIdCapacity(long capacity, long estimatedCount);

        void mayExceedEdgeIdCapacity(long capacity, long estimatedCount);
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void mayExceedEdgeIdCapacity( long capacity, long estimatedCount )
        {   // no-op
        }

        @Override
        public void mayExceedNodeIdCapacity( long capacity, long estimatedCount )
        {   // no-op
        }

        @Override
        public void doubleEdgeRecordUnitsEnabled()
        {   // no-op
        }
    };

    private final File storeDir;
    private final FileSystem fileSystem;
    private final Configuration config;
    private final Log log;
    private final ExecutionMonitor executionMonitor;
    private final DataImporter.Monitor storeUpdateMonitor = new DataImporter.Monitor();
    private final long maxMemory;
    private final Dependencies dependencies = new Dependencies();
    private final Monitor monitor;
    private Input input;

    // This map contains additional state that gets populated, created and used throughout the stages.
    // The reason that this is a map is to allow for a uniform way of accessing and loading this stage
    // from the outside. Currently these things live here:
    //   - EdgeLabelDistribution
    private final Map<Class<?>,Object> accessibleState = new HashMap<>();

    // components which may get assigned and unassigned in some methods
    private long startTime;
    private NumberArrayFactory numberArrayFactory;
    private Collector badCollector;
    private IdMapper<String> idMapper;
    private long peakMemoryUsage;
    private long availableMemoryForLinking;
    private StandardJanusGraph graph;
    private BulkIdAssigner idAssigner;
    private ImportStore janusStore;


    /**
     * @param storeDir directory which the db will be created in.
     * @param fileSystem {@link FileSystem} that the {@code storeDir} lives in.
     * @param config import-specific {@link Configuration}.
     * @param logService {@link LogService} to use.
     * @param executionMonitor {@link ExecutionMonitor} to follow progress as the import proceeds.
     * @param monitor {@link Monitor} for some events.
     */
    public ImportLogic(File storeDir, FileSystem fileSystem,
                       Configuration config, LogService logService, ExecutionMonitor executionMonitor,
                       Monitor monitor ,
                       StandardJanusGraph graph, BulkIdAssigner idAssigner,
                       ImportStore janusStore)
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.monitor = monitor;
        this.log = logService.getInternalLogProvider().getLog( getClass() );
        this.executionMonitor = ExecutionSupervisors.withDynamicProcessorAssignment( executionMonitor, config );
        this.maxMemory = config.maxMemoryUsage();
        this.graph = graph;
        this.idAssigner = idAssigner;
        this.janusStore = janusStore;
    }

    public void initialize( Input input ) throws Exception
    {
        log.info( "Import starting" );
        startTime = currentTimeMillis();
        this.input = input;
        numberArrayFactory = NumberArrayFactory.OFF_HEAP;

//        numberArrayFactory = auto( null, storeDir, config.allowCacheAllocationOnHeap() );
        badCollector = input.badCollector();
        // Some temporary caches and indexes in the import
        idMapper = input.idMapper( numberArrayFactory );

        Input.Estimates inputEstimates = input.calculateEstimates( (value) -> 0 );

        dependencies.satisfyDependencies( inputEstimates, idMapper );


        executionMonitor.initialize( dependencies );
    }

    /**
     * Accesses state of a certain {@code type}. This is state that may be long- or short-lived and perhaps
     * created in one part of the import to be used in another.
     *
     * @param type {@link Class} of the state to get.
     * @return the state of the given type.
     * @throws IllegalStateException if the state of the given {@code type} isn't available.
     */
    public <T> T getState( Class<T> type )
    {
        return type.cast( accessibleState.get( type ) );
    }

    /**
     * Puts state of a certain type.
     *
     * @param state state instance to set.
     * @see #getState(Class)
     * @throws IllegalStateException if state of this type has already been defined.
     */
    public <T> void putState( T state )
    {
        accessibleState.put( state.getClass(), state );
        dependencies.satisfyDependency( state );
    }

    /**
     * Imports nodes w/ their properties and labels from {@link Input#nodes()}. This will as a side-effect populate the {@link IdMapper},
     * to later be used for looking up ID --> nodeId in {@link #importEdges()}. After a completed node import,
     * {@link #prepareIdMapper()} must be called.
     *
     * @throws IOException on I/O error.
     */
    public void importNodes() throws IOException
    {
        // Import nodes, properties
        DataImporter.importNodes( config.maxNumberOfProcessors(), input, idMapper, // config.outputDir()
              executionMonitor, storeUpdateMonitor, graph, idAssigner,
                janusStore);
        updatePeakMemoryUsage();
    }

    /**
     * Prepares {@link IdMapper} to be queried for ID --> nodeId lookups. This is required for running {@link #importEdges()}.
     */
    public void prepareIdMapper()
    {
        if ( idMapper.needsPreparation() )
        {
            MemoryUsageStatsProvider memoryUsageStats = new MemoryUsageStatsProvider( idMapper );

            executeStage( new IdMapperPreparationStage( config, idMapper, badCollector, memoryUsageStats ) );

            PrimitiveLongIterator duplicateNodeIds = idMapper.leftOverDuplicateNodesIds();
            if ( duplicateNodeIds.hasNext() )
            {
                // TODO delete duplicate data
                //executeStage( new DeleteDuplicateNodesStage( config, duplicateNodeIds, janusStore, storeUpdateMonitor ) );
            }
            updatePeakMemoryUsage();
        }
    }

    /**
     * Uses {@link IdMapper} as lookup for ID --> nodeId and imports all edges from {@link Input#edges()}
     * and writes them into the  cassandra .
     *
     * @throws IOException on I/O error.
     */
    public void importEdges() throws IOException
    {
        // Import edges (unlinked), properties
        DataStatistics typeDistribution = DataImporter.importEdges(
                config.maxNumberOfProcessors(),
                input, idMapper, badCollector, executionMonitor, storeUpdateMonitor,
                graph, idAssigner,
                janusStore);

        updatePeakMemoryUsage();
        idMapper.close();
        idMapper = null;
        putState( typeDistribution );
    }


    @Override
    public void close() throws IOException
    {
        // We're done, do some final logging about it
        long totalTimeMillis = currentTimeMillis() - startTime;
        executionMonitor.done( totalTimeMillis, format( "%n%s%nPeak memory usage: %s", storeUpdateMonitor, bytes( peakMemoryUsage ) ) );
        log.info( "<Generate SSTable Files> completed successfully, took " + duration( totalTimeMillis ) + ". " + storeUpdateMonitor );

        if ( idMapper != null )
        {
            idMapper.close();
        }
    }



    private void updatePeakMemoryUsage()
    {
        peakMemoryUsage = max( peakMemoryUsage, totalMemoryUsageOf(idMapper ) );
    }



    private static long totalMemoryUsageOf( MemoryStatsVisitor.Visitable... users )
    {
        GatheringMemoryStatsVisitor total = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable user : users )
        {
            if ( user != null )
            {
                user.acceptMemoryStatsVisitor( total );
            }
        }
        return total.getHeapUsage() + total.getOffHeapUsage();
    }



    private void executeStage( Stage stage )
    {
        superviseExecution( executionMonitor, config, stage );
    }
}
