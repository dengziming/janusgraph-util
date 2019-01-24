package janusgraph.util.batchimport.unsafe.graph.store;

import janusgraph.util.batchimport.unsafe.graph.store.cassandra.CassandraSSTableWriter;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVEntryMutation;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BUFFER_SIZE;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class ImportStores {


    public static abstract class AbstractImportStoreImpl implements ImportStore {

        protected StandardJanusGraph graph;
        int numMutations;
        int persistChunkSize;
        boolean open;
        Map<String, Map<StaticBuffer, KCVEntryMutation>> mutations ;
        protected StoreConsumers.KeyColumnValueConsumer consumer;
        String edgestore ; // default value "edgestore"

        AbstractImportStoreImpl(StandardJanusGraph graph, String edgestore) {
            this.graph = graph;
            this.numMutations = 0;
            this.persistChunkSize = graph.getConfiguration().getConfiguration().get(BUFFER_SIZE);
            this.mutations = new HashMap<>(2);// edgestore and graphindex, here isn't graphindex
            this.open = true;
            this.edgestore = edgestore;
        }

        @Override
        public void mutateEdges(StaticBuffer key, List<Entry> additions) throws BackendException {

            if (additions.isEmpty()) return;

            // cf -> key-> cassmutation
            KCVEntryMutation m = new KCVEntryMutation(additions, new ArrayList<>());

            // without meaning because edgestore is always constant?
            Map<StaticBuffer, KCVEntryMutation> storeMutation = mutations.computeIfAbsent(edgestore, k -> new HashMap<>());

            KCVEntryMutation existingM = storeMutation.get(key);
            if (existingM == null) {
                storeMutation.put(key, m);
            } else {
                existingM.merge(m);
            }
            numMutations += m.getTotalMutations();

            if ( numMutations >= persistChunkSize) {
                flushInternal();
            }

        }

        private void clear(){

            // loop to clear, in fact just on key : edgestore
            for (Map<StaticBuffer, KCVEntryMutation> mutationMap: mutations.values()){
                mutationMap.clear();
                numMutations = 0;
            }
        }


        private void flushInternal() throws BackendException {

            // loop to consume, in fact just on key : edgestore
            for (Map<StaticBuffer, KCVEntryMutation> mutationMap: mutations.values()){
                consumer.accept(mutationMap);
            }
            clear();
        }

        public void close() {

            if (open){
                if (numMutations > 0){
                    try {
                        flushInternal();
                    } catch (BackendException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    consumer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                open = false;
            }

        }
    }

    public static class TxImportStoreImpl extends AbstractImportStoreImpl {

        public TxImportStoreImpl(StandardJanusGraph graph, String edgestore) {
            super(graph, edgestore);
            try {
                this.consumer = new StoreConsumers.JanusGraphTxWriter(graph);
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String getPath() {
            throw new UnsupportedOperationException("TxImportStoreImpl doesn't support getpath");
        }

        @Override
        public String getKeySpace() {
            throw new UnsupportedOperationException("TxImportStoreImpl doesn't support getKeySpace");
        }

        @Override
        public String getTable() {
            throw new UnsupportedOperationException("TxImportStoreImpl doesn't support getTable");
        }
    }


    public static class BulkImportStoreImpl extends AbstractImportStoreImpl {


        private String path;
        private String keySpace;


        public BulkImportStoreImpl(StandardJanusGraph graph, String path, String keySpace, String edgestore) {
            super(graph, edgestore);
            this.path = path;
            this.keySpace = keySpace;
            // this.consumer = new StoreConsumers.SingleWriter(graph,edgestore);//
            try {
                this.consumer = new CassandraSSTableWriter(graph,path,keySpace,edgestore);
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String getPath() {
            return path;
        }

        @Override
        public String getKeySpace() {
            return keySpace;
        }

        @Override
        public String getTable() {
            return edgestore;
        }

    }
}
