package janusgraph.util.batchimport.unsafe.load;

import janusgraph.util.batchimport.unsafe.Configuration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 * load file into databases
 */
public interface BulkLoader {

    void load(StandardJanusGraph graph, PrintStream out, PrintStream err, InputStream in, File storeDir, File logsDir,
              Configuration configuration) throws Exception;
}
