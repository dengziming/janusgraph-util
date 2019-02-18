package janusgraph.util.batchimport.unsafe.output;


import janusgraph.util.batchimport.unsafe.DataImporter;
import janusgraph.util.batchimport.unsafe.graph.store.ImportStore;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.input.InputChunk;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.vertices.StandardVertex;

import static java.lang.String.format;

/**
 * Imports edges using data from {@link InputChunk}.
 */
public class EdgeImporter extends EntityImporter
{
    private final IdMapper<String> idMapper;
    private JanusGraphVertex start ;
    private JanusGraphVertex end;
    private final Collector badCollector;

    protected JanusGraphEdge nodeRecord;
    private boolean startNotFound = true;
    private boolean endNotFound = true;


    private long edgeCount;

    // State to keep in the event of bad edges that need to be handed to the Collector
    private Object startId;
    private Group startIdGroup;
    private Object endId;
    private Group endIdGroup;
    private String type;
    private EdgeLabel edgeLabel;

    public EdgeImporter(boolean bulkLoading,
                        int numRunners,
                        int threadNum,
                        String title,
                        IdMapper<String> idMapper,
                        DataImporter.Monitor monitor,
                        Collector badCollector,
                        StandardJanusGraph graph,
                        BulkIdAssigner idAssigner,
                        ImportStore janusStore)
    {
        super(bulkLoading,numRunners,threadNum,title,monitor,graph,idAssigner,janusStore);
        this.idMapper = idMapper;
        this.badCollector = badCollector;
        edgeCount = 0;

        start = null;
        end = null;
    }

    @Override
    public boolean startId( long id )
    {
        ((InternalElement)start).setId(id);
        return true;
    }

    @Override
    public boolean startId( Object id, Group group )
    {

        long nodeId = nodeId( id, group );
        this.startId = id;
        this.startIdGroup = group;
        if (nodeId == IdMapper.ID_NOT_FOUND){
            return false;
        }else {
            startNotFound = false;
            start = new StandardVertex(stx, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);
            ((InternalElement)start).setId(nodeId);
            return true;
        }
    }

    @Override
    public boolean endId( long id )
    {
        ((InternalElement)end).setId(id);
        return true;
    }

    @Override
    public boolean endId( Object id, Group group )
    {

        if (startNotFound){
            return false;
        }
        long nodeId = nodeId( id, group );
        this.endId = id;
        this.endIdGroup = group;
        if (nodeId == IdMapper.ID_NOT_FOUND){
            return false;
        }else {
            endNotFound = false;
            end = new StandardVertex(stx, IDManager.getTemporaryVertexID(IDManager.VertexIDType.NormalVertex, temporaryIds.nextID()), ElementLifeCycle.New);
            ((InternalElement)end).setId(nodeId);
            // edge should be added after endId 。
            nodeRecord = addEdge(start,end,edgeLabel);
            return true;
        }
    }

    private long nodeId( Object id, Group group )
    {
        long nodeId = idMapper.get( (String)id, group );
        if ( nodeId < 0 )
        {
            return IdMapper.ID_NOT_FOUND;
        }

        return nodeId;
    }



    @Override
    public boolean property( String key, Object value )
    {
        if (nodeRecord == null){
            return false;
        }
        PropertyKey propertyKey = getPropertyKey(key);
        ((InternalRelation)nodeRecord).setPropertyDirect(propertyKey,value);
        propertyCount ++;
        return true;
    }


    @Override
    public boolean type( long typeId )
    {
        return true;
    }

    @Override
    public boolean type( String type )
    {
        this.type = type;
        edgeLabel = getEdgeLabel(type);
        return type(edgeLabel.longId());
    }

    @Override
    public void endOfEntity()
    {

        if (startNotFound || endNotFound){
            try {
                badCollector.collectBadEdge( startId, group( startIdGroup ).name(), type, endId, group( endIdGroup ).name(),
                        startNotFound ? startId : endId );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            // count total
            edgeCount++;
            recordCnt ++ ;
            if (recordCnt == BATCH){

                flush();
                recordCnt = 0;
            }
        }
        startId = null;
        startIdGroup = null;
        start = null;
        end = null;
        endId = null;
        endIdGroup = null;
        type = null;
        edgeLabel = null;
        startNotFound = true;
        endNotFound = true;
        super.endOfEntity();
    }

    private Group group( Group group )
    {
        return group != null ? group : Group.GLOBAL;
    }


    @Override
    public void close()
    {
        flush();
        super.close();
        monitor.edgesImported(edgeCount);
    }
}
