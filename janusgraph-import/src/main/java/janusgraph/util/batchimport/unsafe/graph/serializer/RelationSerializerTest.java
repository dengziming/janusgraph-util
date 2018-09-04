package janusgraph.util.batchimport.unsafe.graph.serializer;

import janusgraph.util.batchimport.unsafe.graph.GraphUtil;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDPool;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.vertices.StandardVertex;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public class RelationSerializerTest {

    protected static IDPool temporaryIds = new IDPool() {

        private final AtomicLong counter = new AtomicLong(1);

        @Override
        public long nextID() {
            return counter.getAndIncrement();
        }

        @Override
        public void close() {
            //Do nothing
        }
    };


    public static void main(String[] args) {

        BulkIdAssigner assigner = GraphUtil.getIdAssigner();
        StandardJanusGraph graph = GraphUtil.getGraph();

        JanusGraphManagement mgmt = graph.openManagement();
        if (!mgmt.containsEdgeLabel("PHONE_PHONE_CONTACT")){
            mgmt.makeEdgeLabel("PHONE_PHONE_CONTACT").make();

        }
        if (!mgmt.containsPropertyKey("name")){
            mgmt.makePropertyKey("name").dataType(String.class).make();
        }

        mgmt.commit();


        long count = Long.parseLong(args[0]);
        long graph_time = 0;
        long serial_time = 0;
        long total_count = 0;
        StandardJanusGraphTx stx = (StandardJanusGraphTx) graph.newTransaction();
        EdgeLabel label = graph.openManagement().getEdgeLabel("PHONE_PHONE_CONTACT");
        PropertyKey name = graph.openManagement().getPropertyKey("name");

        long t1 = System.currentTimeMillis();
        for ( int i = 0; i < count ; i++ ) {
            stx.getExistingPropertyKey(name.longId());
        }
        long t2 = System.currentTimeMillis();
        System.out.println("[propertyKey]" + (t2 - t1));

        long t3 = System.currentTimeMillis();
        for ( int i = 0; i < count; i++ ) {
            stx.getExistingEdgeLabel(label.longId());
        }
        long t4 = System.currentTimeMillis();
        System.out.println("[propertyKey]" + (t4 - t3));

        for ( int i = 0; i < count; i++ )
        {

            StandardVertex outVertex = new StandardVertex(stx, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);
            assigner.assignVertexID(outVertex);
            StandardVertex inVertex = new StandardVertex(stx, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);
            assigner.assignVertexID(inVertex);

            StandardEdge edge = new StandardEdge(IDManager.getTemporaryRelationID(temporaryIds.nextID()), label, outVertex, inVertex, ElementLifeCycle.New);
            assigner.assignRelationID(edge);
            RelationSerializer relationSerializer = new RelationSerializer(graph);

            InternalRelationType type = (InternalRelationType) edge.getType();

            for (int pos = 0; pos < edge.getArity(); pos++) {

                long time1 = System.currentTimeMillis();
                graph.getEdgeSerializer().writeRelation(edge, type, pos, stx);
                long time2 = System.currentTimeMillis();
                relationSerializer.writeRelation(edge, type, pos, stx);
                long time3 = System.currentTimeMillis();
                graph_time += (time2 - time1);
                serial_time += (time3 - time2);
                total_count ++;

            }

        }
        System.out.println("[total_count]" + total_count + "[graph_time]" + graph_time + "[serial_time]" + serial_time);
        stx.close();
        GraphUtil.close();
    }
}
