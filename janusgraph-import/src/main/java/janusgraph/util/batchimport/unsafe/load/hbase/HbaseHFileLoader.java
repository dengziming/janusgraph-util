package janusgraph.util.batchimport.unsafe.load.hbase;

import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.load.BulkLoader;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class HbaseHFileLoader implements BulkLoader {

    @Override
    public void load(StandardJanusGraph graph, PrintStream out, PrintStream err, InputStream in, File storeDir, File logsDir, Configuration configuration) {

        // ignore TODO
    }
}
