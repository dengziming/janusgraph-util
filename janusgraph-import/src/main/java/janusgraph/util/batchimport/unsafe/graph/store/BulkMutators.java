package janusgraph.util.batchimport.unsafe.graph.store;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dengziming on 18/08/2018.
 * mutators to serialize janusgraph element. just like {#EdgeSerialize}
 * use thread pool to speed
 */
public class BulkMutators {



    public static class BulkMutatorImpl implements BulkMutator{

        private StandardJanusGraph graph;
        private ImportStore store;

        public BulkMutatorImpl(StandardJanusGraph graph,ImportStore store) {
            this.graph = graph;
            this.store = store;
        }

        @Override
        public void mutateMany(StandardJanusGraphTx stx, Collection<InternalRelation> addedRelations) throws BackendException {

            ListMultimap<Long, InternalRelation> mutations = ArrayListMultimap.create();

            for (InternalRelation add : addedRelations) {
                Preconditions.checkArgument(add.isNew());

                for (int pos = 0; pos < add.getLen(); pos++) {
                    InternalVertex vertex = add.getVertex(pos);
                    if (pos == 0 || !add.isLoop()) {

                        mutations.put(vertex.longId(), add);
                    }

                }
            }
            addedRelations.clear();

            for (Long vertexId : mutations.keySet()) {
                Preconditions.checkArgument(vertexId > 0, "Vertex has no id: %s", vertexId);
                List<InternalRelation> edges = mutations.get(vertexId);
                List<Entry> additions = new ArrayList<>(edges.size());
                for (InternalRelation edge : edges) {

                    InternalRelationType type = (InternalRelationType) edge.getType();
                    assert type.getBaseType()==null;

                    if (type.getStatus()== SchemaStatus.DISABLED) continue;
                    for (int pos = 0; pos < edge.getArity(); pos++) {
                        if (!type.isUnidirected(Direction.BOTH) && !type.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered
                        if (edge.getVertex(pos).longId()==vertexId) {

                            // FIXME here is some bug ,for example, if a property with integer type and string value,
                            // then the serialize will failed and without throw an Exception! to force cast or print!
                            StaticArrayEntry entry = graph.getEdgeSerializer().writeRelation(edge, type, pos, stx);
                            additions.add(entry);
                        }
                    }
                }

                StaticBuffer vertexKey = graph.getIDManager().getKey(vertexId);
                store.mutateEdges(vertexKey, additions);
            }
        }

        @Override
        public void close() {
            try {
                // TODO close multiple time ?
                store.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public String toString() {
            return store.getPath() + File.separator + store.getKeySpace() + File.separator + store.getTable();
        }
    }

    /**
     * serialize janusgraph element in parallel,this is unnecessary
     */
    public static class ParallelBulkMutator implements BulkMutator{

        private BulkMutatorImpl[] mutators ;
        private ExecutorService executor;
        private int parallel;
        private long cursor;
        private StandardJanusGraph graph;

        public ParallelBulkMutator(StandardJanusGraph graph,ImportStore store,int parallel) {

            this.graph = graph;
            this.parallel = parallel;
            mutators = new BulkMutatorImpl[parallel];
            for (int i = 0; i < parallel; i ++){
                mutators[i] = new BulkMutatorImpl(graph,
                        new ImportStores.BulkImportStoreImpl(graph,store.getPath() + "/_" + i,
                                store.getKeySpace(),store.getTable()));
            }
            try {
                store.close(); // because this store will not be used
            } catch (IOException e) {
                e.printStackTrace();
            }
            executor = Executors.newFixedThreadPool(parallel);
        }

        @Override
        public void mutateMany(StandardJanusGraphTx stx, Collection<InternalRelation> addedRelations) throws BackendException {

            int i = (int) (cursor % parallel);
            Runnable runnable = () -> {
                try {
                    mutators[i].mutateMany(stx, addedRelations);
                } catch (BackendException e) {
                    e.printStackTrace();
                }
            };
            executor.submit(runnable);

            cursor ++;
        }

        @Override
        public void close() {

            for (int i = 0; i < parallel; i ++){
                mutators[i].close();
            }
            executor.shutdown();
        }
    }

}
