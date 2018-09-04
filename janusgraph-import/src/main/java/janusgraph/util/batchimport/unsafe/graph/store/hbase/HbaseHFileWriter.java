package janusgraph.util.batchimport.unsafe.graph.store.hbase;

import janusgraph.util.batchimport.unsafe.graph.store.StoreConsumers;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public abstract class HbaseHFileWriter extends StoreConsumers.KeyColumnValueConsumer {

    protected StandardJanusGraph graph;
    protected final Configuration config;

    public HbaseHFileWriter(StandardJanusGraph graph) throws BackendException {
        super(graph);
        this.config = graph.getConfiguration().getConfiguration();
        // TODO
    }


}