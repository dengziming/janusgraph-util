package janusgraph.util.batchimport.unsafe.graph.store.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_KEYSPACE;
import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.PORT_DEFAULT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 * mainly used to drop keyspace before import, this is used to test,
 */
public class TemporaryCassandraThriftManager extends CassandraManager {

    private String host;
    private int port;
    private String keyspaceName;
    private StandardJanusGraph graph;


    public TemporaryCassandraThriftManager(StandardJanusGraph graph){

        this.graph = graph;
        Configuration configuration = graph.getConfiguration().getConfiguration();
        String[] strings = configuration.get(STORAGE_HOSTS);
        this.host = strings[0];
        if (configuration.has(STORAGE_PORT)){
            this.port = configuration.get(STORAGE_PORT);
        }else {
            this.port = PORT_DEFAULT;
        }
        this.keyspaceName = configuration.get(CASSANDRA_KEYSPACE);

    }

    @Override
    public void reCreateGraphIfExists() throws TException, BackendException {

        TTransport tr = new TFramedTransport(new TSocket(host, port));
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        try {
            // Side effect: throws Exception if keyspaceName doesn't exist
            client.set_keyspace(keyspaceName); // Don't remove
            client.send_system_drop_column_family(Backend.EDGESTORE_NAME);
            client.send_system_drop_column_family(Backend.INDEXSTORE_NAME);
            client.send_system_drop_column_family(JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME);
            // client.send_system_add_keyspace(keyspaceName);
        } catch (InvalidRequestException ignored) {
            // ignored
        }
        try {
            // Give time to gracefully complete
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ((KeyColumnValueStoreManager)graph.getBackend().getStoreManager()).openDatabase(Backend.EDGESTORE_NAME, StoreMetaData.EMPTY);
        ((KeyColumnValueStoreManager)graph.getBackend().getStoreManager()).openDatabase(Backend.INDEXSTORE_NAME, StoreMetaData.EMPTY);
        ((KeyColumnValueStoreManager)graph.getBackend().getStoreManager()).openDatabase(JanusGraphConstants.JANUSGRAPH_ID_STORE_NAME, StoreMetaData.EMPTY);

        tr.close();
    }
}
