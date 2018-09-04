package janusgraph.util.batchimport.unsafe.graph.store.cassandra;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVEntryMutation;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class CassandraSSTableWriter extends CassandraWriter {

    private long cnt;
    private CQLSSTableWriter writer;
    private boolean open;
    private final String cf;
    private final String outputDir ;

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    private final String schema;

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    private final String insert_stmt ;


    private final CQLSSTableWriter.Builder builder;

    public CassandraSSTableWriter(StandardJanusGraph graph,
                                  String path,
                                  String keySpace,
                                  String table) throws BackendException {
        super(graph);

        open = true;
        cnt = 0;
        this.cf = table;
        this.keySpaceName = keySpace;
        this.outputDir = String.format("%s/%s/%s", path,keySpace, cf);

        File outPath = new File(outputDir);
        if (!outPath.exists()){
            outPath.mkdirs();
        }

        String canonical_compaction_strategy = compaction_strategy;
        try {
            canonical_compaction_strategy = CFMetaData.createCompactionStrategy(compaction_strategy).getCanonicalName();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        schema = createStatement(this.keySpaceName, this.cf,canonical_compaction_strategy ,compressionClass);

        insert_stmt = String.format("INSERT INTO %s.%s (" +
                "key, column1, value" +
                ") VALUES (" +
                "?, ?, ?" +
                ")", keySpace, table);


        // Prepare SSTable writer
        builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
                // set target schema
                .forTable(schema)
                // set CQL statement to put data
                .using(insert_stmt)
                // set partitioner if needed
                // default is Murmur3Partitioner so set if you use different one.
                .withPartitioner(new Murmur3Partitioner());
        writer = builder.build();
    }


    @Override
    public void accept(Map<StaticBuffer, KCVEntryMutation> batch) {

        // magic!
        Config.setClientMode(true);

        try {

            // key -> cf -> cassmutation
            for (Map.Entry<StaticBuffer, KCVEntryMutation> cfMutationEntry: batch.entrySet()){

                ByteBuffer keyBB = cfMutationEntry.getKey().asByteBuffer();

                for (Entry mut: cfMutationEntry.getValue().getAdditions()){

                    ByteBuffer columnAs = mut.getColumnAs(StaticBuffer.BB_FACTORY);
                    ByteBuffer valueAs = mut.getValueAs(StaticBuffer.BB_FACTORY);

                    writer.rawAddRow(keyBB, columnAs, valueAs);
                    cnt ++;
                }
            }
        } catch (org.apache.cassandra.exceptions.InvalidRequestException | IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close(){
        if (open){
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            open = false;
        }
    }


    /**

     code from cassandra

     def as_cql_query(self, formatted=False):
     """
     Returns a CQL query that can be used to recreate this table (index
     creations are not included).  If `formatted` is set to :const:`True`,
     extra whitespace will be added to make the query human readable.
     """
     ret = "CREATE TABLE %s.%s (%s" % (
     protect_name(self.keyspace_name),
     protect_name(self.name),
     "\n" if formatted else "")

     if formatted:
     column_join = ",\n"
     padding = "    "
     else:
     column_join = ", "
     padding = ""

     columns = []
     for col in self.columns.values():
     columns.append("%s %s%s" % (protect_name(col.name), col.cql_type, ' static' if col.is_static else ''))

     if len(self.partition_key) == 1 and not self.clustering_key:
     columns[0] += " PRIMARY KEY"

     ret += column_join.join("%s%s" % (padding, col) for col in columns)

     # primary key
     if len(self.partition_key) > 1 or self.clustering_key:
     ret += "%s%sPRIMARY KEY (" % (column_join, padding)

     if len(self.partition_key) > 1:
     ret += "(%s)" % ", ".join(protect_name(col.name) for col in self.partition_key)
     else:
     ret += protect_name(self.partition_key[0].name)

     if self.clustering_key:
     ret += ", %s" % ", ".join(protect_name(col.name) for col in self.clustering_key)

     ret += ")"

     # properties
     ret += "%s) WITH " % ("\n" if formatted else "")
     ret += self._property_string(formatted, self.clustering_key, self.options, self.is_compact_storage)

     return ret
     */
    private String createStatement(String keySpace, String cf, String compaction_class, String compression){

        String schema = String.format("CREATE TABLE %s.%s (" +
                "key blob, " +
                "column1 blob, " +
                "value blob , " +
                "PRIMARY KEY (key, column1)" +
                ")", keySpace, cf);

        String format = String.format("WITH COMPACT STORAGE" +
                "    AND compaction = {'class': '%s'}" + // org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy
                "    AND compression = {'sstable_compression':'%s'}" //LZ4Compressor
                , compaction_class, compression);

        return schema + format;
    }

}
