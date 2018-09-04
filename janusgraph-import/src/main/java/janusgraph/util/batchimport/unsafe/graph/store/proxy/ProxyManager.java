package janusgraph.util.batchimport.unsafe.graph.store.proxy;

import janusgraph.util.batchimport.unsafe.graph.store.StoreManager;
import janusgraph.util.batchimport.unsafe.graph.store.cassandra.TemporaryCassandraThriftManager;
import org.apache.thrift.TException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class ProxyManager implements StoreManager {

    private StoreManager real;

    public ProxyManager(StandardJanusGraph graph){

        String backend = graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND);

        if ("cassandrathrift".equals(backend)){
            real = new TemporaryCassandraThriftManager(graph);
        }else if ("hbase".equals(backend)){
            // ignore
        }else if ("bigtable".equals(backend)){
            // ignore
        }else {

        }
    }

    @Override
    public void reCreateGraphIfExists() throws TException, BackendException {
        real.reCreateGraphIfExists();
    }
}
