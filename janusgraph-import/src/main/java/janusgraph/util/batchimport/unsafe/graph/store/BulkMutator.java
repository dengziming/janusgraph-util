package janusgraph.util.batchimport.unsafe.graph.store;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Collection;

/**
 * Created by dengziming on 18/08/2018.
 * ${Main}
 */
public interface BulkMutator {

    public void mutateMany(StandardJanusGraphTx stx, Collection<InternalRelation> addedRelations) throws BackendException;
    public void close();
}
