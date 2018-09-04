package janusgraph.util.batchimport.unsafe;


import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.io.fs.FileSystem;
import janusgraph.util.batchimport.unsafe.log.LogService;
import janusgraph.util.batchimport.unsafe.stage.ExecutionMonitor;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;

public class StandardBatchImporterFactory extends BatchImporterFactory
{
    @Override
    public BatchImporter instantiate(File storeDir, FileSystem fileSystem, Configuration config,
                                     LogService logService, ExecutionMonitor executionMonitor,
                                     ImportLogic.Monitor monitor,
                                     StandardJanusGraph graph, BulkIdAssigner idAssigner )
    {
        return new ParallelBatchImporter( storeDir, fileSystem, config, logService, executionMonitor,
                 monitor,graph, idAssigner );
    }
}
