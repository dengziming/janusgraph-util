package janusgraph.util.batchimport.unsafe.output;

import janusgraph.util.batchimport.unsafe.DataImporter;
import janusgraph.util.batchimport.unsafe.graph.store.ImportStore;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.input.Group;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.vertices.StandardVertex;

import static java.lang.Long.max;
import static java.util.Arrays.copyOf;

/**
 * @author dengziming (swzmdeng@163.com)
 */
public class NodeImporter extends EntityImporter{

    private final IdMapper<String> idMapper;

    private String[] labels = new String[10];
    private int labelsCursor;

    protected long nodeCount = 0;
    private long highestId = -1;
    private boolean hasLabelField;
    private JanusGraphVertex nodeRecord;

    public NodeImporter(boolean bulkLoading,
                        int numRunners,
                        int threadNum,
                        String title,
                        IdMapper<String> idMapper,
                        DataImporter.Monitor monitor ,
                        StandardJanusGraph graph,
                        BulkIdAssigner idAssigner,
                        ImportStore janusStore
                        ) {
        super(bulkLoading,numRunners,threadNum,title,monitor,graph,idAssigner,janusStore);
        this.idMapper = idMapper;

        nodeRecord = new StandardVertex(stx, -1, ElementLifeCycle.New);// temp id
    }


    @Override
    public boolean id( long id )
    {
        highestId = max( highestId, id );
        return true;
    }

    @Override
    public boolean id( Object id, Group group )
    {

        //
        VertexLabel vertexLabel = getVertexLabel(group.name());

        nodeRecord = addVertex(stx,vertexLabel);// tmp id

        idMapper.put( (String) id, group, nodeRecord.longId() );

        return true;
    }

    @Override
    public boolean property(PropertyKey key, Object value )
    {
        propertyCount ++;
        addProperty(nodeRecord,key,value);
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        assert !hasLabelField;
        if ( labelsCursor + labels.length > this.labels.length )
        {
            this.labels = copyOf( this.labels, this.labels.length * 2 );
        }
        System.arraycopy( labels, 0, this.labels, labelsCursor, labels.length );
        labelsCursor += labels.length;
        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        hasLabelField = true;
        return true;
    }

    @Override
    public void endOfEntity()
    {
        nodeCount ++;
        recordCnt ++;

        if (recordCnt == BATCH){

            flush();
            recordCnt = 0;
        }
        /*if (nodeCount % 10_000_000 == 0){
            System.out.println("handle 10_000_000 entity");
        }*/
        super.endOfEntity();
    }

    @Override
    public void close()
    {
        flush();
        super.close();
        monitor.nodesImported( nodeCount );
    }

}
