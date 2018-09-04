package janusgraph.util.batchimport.unsafe.graph.store;


import org.apache.thrift.TException;
import org.janusgraph.diskstorage.BackendException;

public interface StoreManager {

    void reCreateGraphIfExists() throws TException, BackendException;
}
