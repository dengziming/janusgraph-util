package janusgraph.util.batchimport.unsafe.graph.store;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVEntryMutation;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class StoreConsumers {


    public abstract static class KeyColumnValueConsumer implements Consumer<Map<StaticBuffer, KCVEntryMutation>>,Closeable {

        protected StandardJanusGraph graph;
        public KeyColumnValueConsumer(StandardJanusGraph graph) throws BackendException {

            this.graph = graph;
        }
    }

    /**
     * simple writer use BackendTransaction
     */
    public static class JanusGraphTxWriter extends KeyColumnValueConsumer{


        private List<Entry> EMPTY = new ArrayList<>();

        public JanusGraphTxWriter(StandardJanusGraph graph) throws BackendException {
            super(graph);
        }

        @Override
        public void accept(Map<StaticBuffer, KCVEntryMutation> mutationMap) {

            StandardJanusGraphTx tx = (StandardJanusGraphTx)graph.newTransaction();

            BackendTransaction mutater = tx.getTxHandle();


            for (Map.Entry<StaticBuffer, KCVEntryMutation> mutEntry : mutationMap.entrySet()) {

                try {
                    mutater.mutateEdges(mutEntry.getKey(), mutEntry.getValue().getAdditions(), EMPTY);
                } catch (BackendException e) {
                    e.printStackTrace();
                }
            }

            try {
                mutater.commit();
                tx.commit();
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {

        }

    }



}
