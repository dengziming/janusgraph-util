package janusgraph.util.batchimport.unsafe.load;

import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.load.cassandra.Cassandra3ShellLoader;
import janusgraph.util.batchimport.unsafe.load.cassandra.CassandraSSTableLoader;
import org.apache.cassandra.utils.FBUtilities;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class ProxyBulkLoader implements BulkLoader {


    private BulkLoader real;

    public ProxyBulkLoader(StandardJanusGraph graph){

        String backend = graph.getConfiguration().getConfiguration().get(STORAGE_BACKEND);

        if ("cassandrathrift".equals(backend)){
            String versionString = FBUtilities.getReleaseVersionString();

            // cassandra3
            if (versionString != null && versionString.startsWith("3")){
                real = new Cassandra3ShellLoader();
            }else {
                real = new CassandraSSTableLoader();
            }
        }else if ("hbase".equals(backend)){
            // ignore
        }else if ("bigtable".equals(backend)){
            // ignore
        }else {
        }
    }

    @Override
    public void load(StandardJanusGraph graph, PrintStream out, PrintStream err, InputStream in, File storeDir, File logsDir,
                     Configuration configuration) throws Exception {

        real.load(graph,out,err,in,storeDir,logsDir,configuration);
    }
}
