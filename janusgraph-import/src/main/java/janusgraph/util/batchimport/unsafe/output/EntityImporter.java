package janusgraph.util.batchimport.unsafe.output;

import janusgraph.util.batchimport.unsafe.DataImporter;
import janusgraph.util.batchimport.unsafe.graph.store.BulkMutator;
import janusgraph.util.batchimport.unsafe.graph.store.BulkMutators;
import janusgraph.util.batchimport.unsafe.graph.store.ImportStore;
import janusgraph.util.batchimport.unsafe.graph.store.ImportStores;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDPool;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.relations.StandardVertexProperty;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex;
import org.janusgraph.graphdb.vertices.StandardVertex;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.String.format;

/**
 * Created by dengziming on 16/08/2018.
 * ${Main}
 */
public abstract class EntityImporter extends InputEntityVisitor.Adapter {

    protected final BulkIdAssigner idAssigner;
    protected final BulkMutator mutator;
    private final ImportStore janusStore;
    protected int recordCnt = 0;
    protected final int BATCH = 10_000;// = 1_000_000; // TODO use config to pass
    protected final Collection<InternalRelation> addedRelations = new ArrayList<>(BATCH);
    protected static final int DEFAULT_CAPACITY = 128;

    private final StandardJanusGraph graph;
    protected final StandardJanusGraphTx stx;
    JanusGraphManagement mgmt ;
    private Map<String,PropertyKey> propertyKeys = new HashMap<>(50);
    private Map<String,EdgeLabel> edgeLabels = new HashMap<>(50);
    private Map<String,VertexLabel> vertexLabels = new HashMap<>(50);

    private int propertyBlocksCursor;
    protected final DataImporter.Monitor monitor;
    protected long propertyCount;
    private boolean hasPropertyId;
    private long propertyId;

    protected LongAdder roughEntityCountProgress = new LongAdder();

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

    protected EntityImporter(int numRunners, // this field is remained to do multi-thread serialize in future edition
                             int rank,
                             String title,
                             DataImporter.Monitor monitor ,
                             StandardJanusGraph graph,
                             BulkIdAssigner idAssigner,
                             ImportStore janusStore)
    {

        this.idAssigner = idAssigner;
        this.monitor = monitor;
        this.graph = graph;
        this.stx = (StandardJanusGraphTx) graph.newTransaction();
        this.mgmt = graph.openManagement();
        this.janusStore = new ImportStores.BulkImportStoreImpl(graph,
                janusStore.getPath() + File.separator + title + File.separator + rank,
                janusStore.getKeySpace(),janusStore.getTable());
        //this.mutator = new BulkMutators.ParallelBulkMutator(graph,janusStore,  2);// use 1/3 of all cores. +1 in case of numRunners<3
        this.mutator = new BulkMutators.BulkMutatorImpl(graph,this.janusStore);
    }


    @Override
    public boolean property( String key, Object value )
    {

        PropertyKey propertyKey = getPropertyKey(key);

        return property(propertyKey, value);
    }

    @Override
    public boolean property( int propertyKeyId, Object value )
    {

        new PropertyKeyVertex(stx, propertyKeyId, ElementLifeCycle.New);
        return true;
    }


    @Override
    public void endOfEntity()
    {
        propertyBlocksCursor = 0;
        hasPropertyId = false;
    }


    @Override
    public void close()
    {
        monitor.propertiesImported( propertyCount );
        stx.commit();
        stx.close();
        mgmt.commit();
        mutator.close();
    }

    public static class Monitor
    {
        private final LongAdder nodes = new LongAdder();
        private final LongAdder edges = new LongAdder();
        private final LongAdder properties = new LongAdder();

        public void nodesImported( long nodes )
        {
            this.nodes.add( nodes );
        }

        public void nodesRemoved( long nodes )
        {
            this.nodes.add( -nodes );
        }

        public void edgesImported(long edges )
        {
            this.edges.add( edges );
        }

        public void propertiesImported( long properties )
        {
            this.properties.add( properties );
        }

        public void propertiesRemoved( long properties )
        {
            this.properties.add( -properties );
        }

        public long nodesImported()
        {
            return this.nodes.sum();
        }

        public long propertiesImported()
        {
            return this.properties.sum();
        }

        public long edgesImported()
        {
            return this.edges.sum();
        }

        @Override
        public String toString()
        {
            return format( "Imported:%n  %d nodes%n  %d edges%n  %d properties",
                    nodes.sum(), edges.sum(), properties.sum() );
        }
    }



    /**
     * tools to add Vertex
     * @param tx
     * @param vertexLabel
     * @return
     */
    protected StandardVertex addVertex(StandardJanusGraphTx tx, VertexLabel vertexLabel){
        StandardVertex vertex = new StandardVertex(tx, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);

        assignID(vertex);
        // also set the label
        addProperty(vertex, BaseKey.VertexExists, Boolean.TRUE);
        addEdge(vertex, vertexLabel, BaseLabel.VertexLabelEdge);
        return vertex;
    }

    protected void addProperty(JanusGraphVertex vertex, PropertyKey key, Object value){
        StandardVertexProperty prop = new StandardVertexProperty(IDManager.getTemporaryRelationID(temporaryIds.nextID()), key, (InternalVertex) vertex, value, ElementLifeCycle.New);
        assignID(prop);
        connectRelation(prop);
    }

    /**
     *
     * add edge
     */
    protected JanusGraphEdge addEdge(JanusGraphVertex outVertex, JanusGraphVertex inVertex, EdgeLabel label){

        StandardEdge edge = new StandardEdge(IDManager.getTemporaryRelationID(temporaryIds.nextID()), label, (InternalVertex) outVertex, (InternalVertex) inVertex, ElementLifeCycle.New);
        assignID(edge);
        connectRelation(edge);
        return edge;
    }

    protected VertexLabel getVertexLabel(String label){

        vertexLabels.computeIfAbsent(label, k -> mgmt.getVertexLabel(label));
        return vertexLabels.get(label);
    }

    protected EdgeLabel getEdgeLabel(String label){

        edgeLabels.computeIfAbsent(label, k -> mgmt.getEdgeLabel(label));
        return edgeLabels.get(label);
    }

    protected PropertyKey getPropertyKey(String property){

        propertyKeys.computeIfAbsent(property, k -> mgmt.getPropertyKey(property));
        return propertyKeys.get(property);
    }


    protected void assignID(InternalVertex vertex) {
        idAssigner.assignVertexID(vertex);
    }

    protected void assignID(InternalRelation relation) {
        idAssigner.assignRelationID(relation);
    }

    protected void connectRelation(InternalRelation r) {

        addedRelations.add(r);
    }

    protected void flush() {

        try {
            mutator.mutateMany(stx,addedRelations);
        } catch (BackendException e) {
            e.printStackTrace();
        }
    }

}
