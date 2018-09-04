package janusgraph.util.batchimport.unsafe;


import janusgraph.util.batchimport.unsafe.graph.store.ImportStore;
import janusgraph.util.batchimport.unsafe.graph.store.ImportStores;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.input.csv.Input;
import janusgraph.util.batchimport.unsafe.io.fs.FileSystem;
import janusgraph.util.batchimport.unsafe.lifecycle.LifecycleAdapter;
import janusgraph.util.batchimport.unsafe.log.LogService;
import janusgraph.util.batchimport.unsafe.stage.ExecutionMonitor;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;


/**
 * {@link BatchImporter} which tries to exercise as much of the available resources to gain performance.
 * Or rather ensure that the slowest resource (usually I/O) is fully saturated and that enough work is
 * being performed to keep that slowest resource saturated all the time.
 * <p>
 * Overall goals: split up processing cost by parallelizing. Keep CPUs busy, keep I/O busy and writing sequentially.
 * I/O is only allowed to be read to and written from sequentially, any random access drastically reduces performance.
 * Goes through multiple stages where each stage has one or more steps executing in parallel, passing
 * batches between these steps through each stage, i.e. passing batches downstream.
 */
public class ParallelBatchImporter extends LifecycleAdapter implements BatchImporter
{
    private final File storeDir;
    private final FileSystem fileSystem;
    private final Configuration config;
    private final LogService logService;
    private final ExecutionMonitor executionMonitor;
    private final ImportLogic.Monitor monitor;
    private StandardJanusGraph graph;
    private BulkIdAssigner idAssigner;

    public ParallelBatchImporter(File storeDir, FileSystem fileSystem,
                                 Configuration config, LogService logService, ExecutionMonitor executionMonitor,
                                 ImportLogic.Monitor monitor ,StandardJanusGraph graph,BulkIdAssigner idAssigner)
    {
        this.storeDir = storeDir;
        this.fileSystem = fileSystem;
        this.config = config;
        this.logService = logService;
        this.executionMonitor = executionMonitor;
        this.monitor = monitor;
        this.graph = graph;
        this.idAssigner = idAssigner;
    }

    @Override
    public void doImport( Input input ) throws Exception
    {

        try (ImportStore janusStore = new ImportStores.BulkImportStoreImpl(graph,
                        storeDir.getPath(),
                        graph.getConfiguration().getConfiguration().get(CASSANDRA_KEYSPACE), //FIXME user determin Keyspace
                        Backend.EDGESTORE_NAME);
             ImportLogic logic = new ImportLogic( storeDir, fileSystem, config, logService,
                      executionMonitor, monitor,graph,idAssigner ,janusStore ) )
        {
            logic.initialize( input );
            long time1 = System.currentTimeMillis();
            logic.importNodes();
            long time2 = System.currentTimeMillis();
            logic.prepareIdMapper();
            long time3 = System.currentTimeMillis();
            logic.importRelationships();
            long time4 = System.currentTimeMillis();
            System.out.println("[node]" + (time2 - time1) + "[map]" + (time3 - time2) + "[relation]" + (time4 - time3)) ;
            /*logic.calculateNodeDegrees();
            logic.linkRelationshipsOfAllTypes();
            logic.defragmentRelationshipGroups();
            logic.buildCountsStore();*/
        }
    }
}
