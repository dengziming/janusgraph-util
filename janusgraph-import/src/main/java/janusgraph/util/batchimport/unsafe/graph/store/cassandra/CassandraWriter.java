package janusgraph.util.batchimport.unsafe.graph.store.cassandra;

import janusgraph.util.batchimport.unsafe.graph.store.StoreConsumers;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.*;


/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public abstract class CassandraWriter extends StoreConsumers.KeyColumnValueConsumer {

    protected String keySpaceName;
    protected final Configuration config;
    protected final String compressionClass;
    protected final String compaction_strategy;
    protected final Map<String, String> compactionOptions;
    protected final Map<String, String> strategyOptions;



    public CassandraWriter(StandardJanusGraph graph) throws BackendException {
        super(graph);
        config = graph.getConfiguration().getConfiguration();
        this.keySpaceName = config.get(CASSANDRA_KEYSPACE);
        this.compressionClass = config.get(CF_COMPRESSION_TYPE);

        if (config.has(COMPACTION_STRATEGY)) {
            compaction_strategy = config.get(COMPACTION_STRATEGY);
        }else {
            compaction_strategy = "SizeTieredCompactionStrategy";
        }

        // get some config
        if (config.has(COMPACTION_OPTIONS)) {
            String[] options = config.get(COMPACTION_OPTIONS);

            if (options.length % 2 != 0)
                throw new IllegalArgumentException(COMPACTION_OPTIONS.getName() + " should have even number of elements.");

            Map<String, String> converted = new HashMap<String, String>(options.length / 2);

            for (int i = 0; i < options.length; i += 2) {
                converted.put(options[i], options[i + 1]);
            }

            this.compactionOptions = ImmutableMap.copyOf(converted);
        } else {
            this.compactionOptions = ImmutableMap.of();
        }

        if (config.has(REPLICATION_OPTIONS)) {
            String[] options = config.get(REPLICATION_OPTIONS);

            if (options.length % 2 != 0)
                throw new IllegalArgumentException(REPLICATION_OPTIONS.getName() + " should have even number of elements.");

            Map<String, String> converted = new HashMap<String, String>(options.length / 2);

            for (int i = 0; i < options.length; i += 2) {
                converted.put(options[i], options[i + 1]);
            }

            this.strategyOptions = ImmutableMap.copyOf(converted);
        } else {
            this.strategyOptions = ImmutableMap.of("replication_factor", String.valueOf(config.get(REPLICATION_FACTOR)));
        }


    }
}
