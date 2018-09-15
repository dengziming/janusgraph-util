package janusgraph.util.batchimport.unsafe;


import janusgraph.util.batchimport.unsafe.graph.store.ImportStore;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.input.*;
import janusgraph.util.batchimport.unsafe.input.csv.Input;
import janusgraph.util.batchimport.unsafe.io.IoMonitor;
import janusgraph.util.batchimport.unsafe.io.IoTracer;
import janusgraph.util.batchimport.unsafe.output.EntityImporter;
import janusgraph.util.batchimport.unsafe.output.NodeImporter;
import janusgraph.util.batchimport.unsafe.output.EdgeImporter;
import janusgraph.util.batchimport.unsafe.stage.ExecutionMonitor;
import janusgraph.util.batchimport.unsafe.stage.StageExecution;
import janusgraph.util.batchimport.unsafe.stage.Step;
import janusgraph.util.batchimport.unsafe.stats.*;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static janusgraph.util.batchimport.unsafe.stats.Stats.longStat;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

/**
 * Imports data from {@link Input} into a store. Only linkage between property records is done, not between nodes/edges
 * or any other types of records.
 *
 * Main design goal here is low garbage and letting multiple threads import with as little as possible shared between threads.
 * So importing consists of instantiating an input source reader, optimal number of threads and letting each thread:
 * <ol>
 * <li>Get {@link InputChunk chunk} of data and for every entity in it:</li>
 * <li>Parse its data, filling current record with data using {@link InputEntityVisitor} callback from parsing</li>
 * <li>Write record(s)</li>
 * <li>Repeat until no more chunks from input.</li>
 * </ol>
 */
public class DataImporter
{
    public static final String NODE_IMPORT_NAME = "Nodes";
    public static final String EDGE_IMPORT_NAME = "Edges";

    public static class Monitor
    {
        private final LongAdder nodes = new LongAdder();
        private final LongAdder edges = new LongAdder();
        private final LongAdder properties = new LongAdder();

        public void nodesImported( long nodes )
        {
            this.nodes.add( nodes );
        }

        public void nodesRemoved( long nodes )
        {
            this.nodes.add( -nodes );
        }

        public void edgesImported(long edges )
        {
            this.edges.add( edges );
        }

        public void propertiesImported( long properties )
        {
            this.properties.add( properties );
        }

        public void propertiesRemoved( long properties )
        {
            this.properties.add( -properties );
        }

        public long nodesImported()
        {
            return this.nodes.sum();
        }

        public long propertiesImported()
        {
            return this.properties.sum();
        }

        public long edgesImported()
        {
            return this.edges.sum();
        }

        @Override
        public String toString()
        {
            return format( "Imported:%n  %d nodes%n  %d edges%n  %d properties",
                    nodes.sum(), edges.sum(), properties.sum() );
        }
    }

    private static long importData(String title, int numRunners, InputIterable data,
                                   Function<Integer,EntityImporter> visitors, ExecutionMonitor executionMonitor, StatsProvider memoryStatsProvider )
            throws IOException
    {
        LongAdder roughEntityCountProgress = new LongAdder();
        ExecutorService pool = Executors.newFixedThreadPool( numRunners,
                new NamedThreadFactory( title + "Importer" ) );
        IoMonitor writeMonitor = new IoMonitor(IoTracer.NONE);
        ControllableStep step = new ControllableStep( title, roughEntityCountProgress, Configuration.DEFAULT,
                writeMonitor, memoryStatsProvider );
        StageExecution execution = new StageExecution( title, null, Configuration.DEFAULT, Collections.singletonList( step ), 0 );
        long startTime = currentTimeMillis();
        try ( InputIterator dataIterator = data.iterator() )
        {
            for ( int i = 0; i < numRunners; i++ )
            {
                pool.submit( new ExhaustingEntityImporterRunnable(
                        execution, dataIterator, visitors.apply(i), roughEntityCountProgress ) );
            }
            pool.shutdown();

            executionMonitor.start( execution );
            long nextWait = 0;
            try
            {
                while ( !pool.awaitTermination( nextWait, TimeUnit.MILLISECONDS ) )
                {
                    executionMonitor.check( execution );
                    nextWait = executionMonitor.nextCheckTime() - currentTimeMillis();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new IOException( e );
            }
        }

        execution.assertHealthy();
        step.markAsCompleted();
        writeMonitor.stop();
        executionMonitor.end( execution, currentTimeMillis() - startTime );
        execution.assertHealthy();

        return roughEntityCountProgress.sum();
    }

    public static void importNodes(int numRunners, Input input, IdMapper<String> idMapper,
                                   ExecutionMonitor executionMonitor, Monitor monitor ,
                                   StandardJanusGraph graph ,BulkIdAssigner idAssigner,
                                   ImportStore janusStore
                                   )
                    throws IOException
    {
        Function<Integer,EntityImporter> importers = (i) -> new NodeImporter(numRunners,i,NODE_IMPORT_NAME, idMapper, monitor,
                graph,idAssigner,janusStore);
        importData( NODE_IMPORT_NAME, numRunners, input.nodes(), importers, executionMonitor,
                new MemoryUsageStatsProvider( idMapper ) );
    }

    public static DataStatistics importEdges(int numRunners, Input input,
                                             IdMapper<String> idMapper, Collector badCollector, ExecutionMonitor executionMonitor,
                                             Monitor monitor,
                                             StandardJanusGraph graph ,
                                             BulkIdAssigner idAssigner,
                                             ImportStore janusStore)
                    throws IOException
    {
        DataStatistics typeDistribution = new DataStatistics( monitor.nodes.sum(), monitor.properties.sum(),
                new DataStatistics.EdgeTypeCount[0] );
        Function<Integer,EntityImporter> importers = (i) -> new EdgeImporter(numRunners,i, EDGE_IMPORT_NAME, idMapper, monitor,
                badCollector , graph , idAssigner,janusStore);
        importData(EDGE_IMPORT_NAME, numRunners, input.edges(), importers, executionMonitor,
                new MemoryUsageStatsProvider( idMapper ) );
        return typeDistribution;
    }

    /**
     * Here simply to be able to fit into the ExecutionMonitor thing
     */
    private static class ControllableStep implements Step<Void>, StatsProvider
    {
        private final String name;
        private final LongAdder progress;
        private final int batchSize;
        private final Key[] keys = new Key[] {Keys.done_batches, Keys.avg_processing_time};
        private final Collection<StatsProvider> statsProviders = new ArrayList<>();

        private volatile boolean completed;

        ControllableStep( String name, LongAdder progress, Configuration config, StatsProvider... additionalStatsProviders )
        {
            this.name = name;
            this.progress = progress;
            this.batchSize = config.batchSize(); // just to be able to report correctly

            statsProviders.add( this );
            statsProviders.addAll( Arrays.asList( additionalStatsProviders ) );
        }

        void markAsCompleted()
        {
            this.completed = true;
        }

        @Override
        public void receivePanic( Throwable cause )
        {
        }

        @Override
        public void start( int orderingGuarantees )
        {
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public long receive( long ticket, Void batch )
        {
            return 0;
        }

        @Override
        public StepStats stats()
        {
            return new StepStats( name, completed, statsProviders );
        }

        @Override
        public void endOfUpstream()
        {
        }

        @Override
        public boolean isCompleted()
        {
            return completed;
        }

        @Override
        public void setDownstream( Step<?> downstreamStep )
        {
        }

        @Override
        public void close() throws Exception
        {
        }

        @Override
        public Stat stat(Key key )
        {
            if ( key == Keys.done_batches )
            {
                return longStat( progress.sum() / batchSize );
            }
            if ( key == Keys.avg_processing_time )
            {
                return longStat( 10 );
            }
            return null;
        }

        @Override
        public Key[] keys()
        {
            return keys;
        }
    }
}
