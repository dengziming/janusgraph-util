package janusgraph.util.batchimport.unsafe.idassigner;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDPool;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_RENEW_BUFFER_PERCENTAGE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_RENEW_TIMEOUT;

/**
 * Created by dengziming on 15/08/2018.
 * this is different from {@code VertexIDAssigner}, VertexIDAssigner just assign unique id, and is single thread
 * BulkIdAssigner should try best to make sure the id is self increase to make use of all long number, and we will
 * sort the number
 */
public class BulkIdAssigner  {

    private StandardJanusGraph graph;
    private Configuration config ;
    private int partitionBits;
    private IDManager idManager;
    private int partitionIdBound;
    private IDAuthority idAuthority;
    private Duration renewTimeoutMS;
    private double renewBufferPercentage;
    private int vertexPartitionID;
    private int relationPartitionID;
    private final IDPool vertexIdPool;
    private final IDPool relationIdPool;

    public BulkIdAssigner(StandardJanusGraph graph){
        this.graph = graph;
        config = graph.getConfiguration().getConfiguration();
//        partitionBits = NumberUtil.getPowerOf2(config.get(CLUSTER_MAX_PARTITIONS));
        partitionBits = 5;
        idManager = new IDManager(partitionBits);
        partitionIdBound = (int)idManager.getPartitionBound();
        idAuthority = graph.getBackend().getIDAuthority();
        renewTimeoutMS = config.get(IDS_RENEW_TIMEOUT);
        renewBufferPercentage = config.get(IDS_RENEW_BUFFER_PERCENTAGE);
        vertexPartitionID = 0;
        relationPartitionID = 1;
        vertexIdPool = new BatchStandardIDPool(graph,idAuthority, vertexPartitionID, 0, idManager.getVertexCountBound(), renewTimeoutMS, renewBufferPercentage);
        relationIdPool = new BatchStandardIDPool(graph,idAuthority, relationPartitionID, 3, idManager.getRelationCountBound(), renewTimeoutMS, renewBufferPercentage);

        Preconditions.checkArgument(vertexPartitionID < partitionIdBound, vertexPartitionID);

    }

    public void assignRelationID(InternalRelation element) {

        assignRelationIDAttempt(element);
    }

    private void assignRelationIDAttempt(final InternalRelation element) {
        Preconditions.checkNotNull(element);
//        Preconditions.checkArgument(!element.hasId());

        long count = relationIdPool.nextID();

        long elementId = idManager.getRelationID(count, relationPartitionID);


        Preconditions.checkArgument(elementId >= 0);
        element.setId(elementId);
    }

    public void assignVertexID(JanusGraphVertex element) {
        assignVertexIDAttempt(element);
    }

    private void assignVertexIDAttempt(final JanusGraphVertex element) {
        Preconditions.checkNotNull(element);
        //Preconditions.checkArgument(!element.hasId());

        long count = vertexIdPool.nextID();

        long elementId = idManager.getVertexID(count, vertexPartitionID, IDManager.VertexIDType.NormalVertex);

        Preconditions.checkArgument(elementId >= 0);
        ((InternalElement)element).setId(elementId);
    }

    public void close() {
        vertexIdPool.close();
        relationIdPool.close();
    }

}
