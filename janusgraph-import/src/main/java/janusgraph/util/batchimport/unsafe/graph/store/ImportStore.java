package janusgraph.util.batchimport.unsafe.graph.store;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;

import java.io.Closeable;
import java.util.List;

/**
 * Created by dengziming on 18/08/2018.
 * ${Main}
 */
public interface ImportStore extends Closeable{

    public String getPath();
    public String getKeySpace();
    public String getTable();
    public void mutateEdges(StaticBuffer key, List<Entry> additions) throws BackendException;

}
