package janusgraph.util.batchimport.unsafe.load.cassandra;

import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.DataImporter;
import janusgraph.util.batchimport.unsafe.helps.ArrayUtil;
import janusgraph.util.batchimport.unsafe.load.BulkLoader;
import org.janusgraph.diskstorage.Backend;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class CassandraSSTableLoader implements BulkLoader {


    @Override
    public void load(StandardJanusGraph graph, PrintStream out, PrintStream err, InputStream in, File storeDir, File logsDir,
                     Configuration configuration) throws Exception {

        /*
         org.apache.cassandra.tools.BulkLoader -d host /path/to/databases/Nodes/0/janusgraph/edgestore/
         org.apache.cassandra.tools.BulkLoader -d host /path/to/databases/Edges/0/janusgraph/edgestore/
         */
        out.println("org.apache.cassandra.tools.BulkLoader");
        org.janusgraph.diskstorage.configuration.Configuration cassConf = graph.getConfiguration().getConfiguration();

        List<String> argList = new ArrayList<>();
        String d = ArrayUtil.join(cassConf.get(STORAGE_HOSTS), ",");
        argList.add("-d");
        argList.add(d);

        Integer p = cassConf.get(STORAGE_PORT);
        String p_string = String.valueOf(p);
        if (p != null && p_string.length() > 0){
            argList.add("-p");
            argList.add(p_string);
        }

        String ts = cassConf.get(SSL_TRUSTSTORE_LOCATION);
        if (ts != null && ts.length() > 0){
            argList.add("-ts");
            argList.add(ts);
        }
        String tspw = cassConf.get(SSL_TRUSTSTORE_PASSWORD);

        if (tspw != null && tspw.length() > 0){
            argList.add("-tspw");
            argList.add(tspw);
        }

        // TODO BulkLoader will call System.exit(),how to solve this.
        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(new NoExitSecurityManager());

        for (int i = 0; i < configuration.maxNumberOfProcessors(); i ++){
            List<String> nodeList = new ArrayList<>(argList);
            String path = storeDir.getAbsolutePath() + File.separator + DataImporter.NODE_IMPORT_NAME +
                    File.separator + i + File.separator + cassConf.get(CASSANDRA_KEYSPACE) + File.separator + Backend.EDGESTORE_NAME;
            nodeList.add(path);

            try {
                org.apache.cassandra.tools.BulkLoader.main(nodeList.toArray(new String[nodeList.size()]));
            } catch (SecurityException e) {
                out.println("this exception doesn't have impact" + e.getClass());
            }
        }

        for (int i = 0; i < configuration.maxNumberOfProcessors(); i ++){
            List<String> relationList = new ArrayList<>(argList);
            String path = storeDir.getAbsolutePath() + File.separator  + DataImporter.EDGE_IMPORT_NAME +
                    File.separator + i + File.separator + cassConf.get(CASSANDRA_KEYSPACE) + File.separator + Backend.EDGESTORE_NAME;
            relationList.add(path);

            try {
                org.apache.cassandra.tools.BulkLoader.main(relationList.toArray(new String[relationList.size()]));
            } catch (SecurityException e) {
                out.println("this exception doesn't have impact" + e.getClass());
            }
        }
        System.setSecurityManager(securityManager);
    }

    static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkPermission(Permission perm) {
            // allow anything.
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
            // allow anything.
        }

        @Override
        public void checkExit(int status) {
            super.checkExit(status);
            throw new SecurityException();
        }
    }
}
