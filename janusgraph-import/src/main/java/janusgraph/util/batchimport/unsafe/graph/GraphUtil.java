package janusgraph.util.batchimport.unsafe.graph;

import janusgraph.util.batchimport.unsafe.helps.Args;
import janusgraph.util.batchimport.unsafe.idassigner.BulkIdAssigner;
import janusgraph.util.batchimport.unsafe.input.csv.Header;
import janusgraph.util.batchimport.unsafe.input.csv.Type;
import org.apache.commons.configuration.BaseConfiguration;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Map;

/**
 * Created by dengziming on 17/08/2018.
 * 进行 janus 操作，
 * 增删查改，建索引，得到图对象
 */
public class GraphUtil {

    private static final Logger logger = LoggerFactory.getLogger(GraphUtil.class);
    public static StandardJanusGraph graph;
    private static BulkIdAssigner idAssigner;


    public static StandardJanusGraph getGraph(String config) {
        if (null == graph){
            synchronized (JanusGraph.class){
                if (null == graph){
                    graph = (StandardJanusGraph)JanusGraphFactory.open(config);

                }
            }
        }
        return graph;
    }

    public static StandardJanusGraph getGraph(Map<String,String> config) {
        if (null == graph){
            synchronized (JanusGraph.class){
                if (null == graph){

                    BaseConfiguration configuration = new BaseConfiguration();
                    for (Map.Entry<String,String> entry: config.entrySet()){
                        configuration.setProperty(entry.getKey(), entry.getValue());
                    }
                    graph = (StandardJanusGraph)JanusGraphFactory.open(configuration);

                }
            }
        }
        return graph;
    }

    public static StandardJanusGraph getGraph() {

        if (graph != null){
            return graph;
        }
        else {
            logger.error("graph hasn't been initialized");
            return getGraph("janusgraph-config.properties");
        }
    }


    private static IDManager getIDManager(){
        return getGraph().getIDManager();
    }

    public static BulkIdAssigner getIdAssigner() {

        if (null == idAssigner){
            synchronized (BulkIdAssigner.class){
                if (null == idAssigner){
                    idAssigner = new BulkIdAssigner(getGraph());
                }
            }
        }
        return idAssigner;
    }

    public static void close(){

        if (null != idAssigner){
            idAssigner.close();
            idAssigner = null;
        }
        if (null != graph){
            graph.tx().rollback();
            graph.close();
            graph = null;
        }
    }



    public static Entry writeRelation(InternalRelation relation, InternalRelationType type, int position, StandardJanusGraphTx tx){

        return getGraph().getEdgeSerializer().writeRelation(relation,position,tx);
    }
    public static void createSchema(Header header) {
        JanusGraphManagement mgmt = getGraph().openManagement();

        for (Header.Entry entry: header.entries()){

            if (entry.type() == Type.ID){
                if (!mgmt.containsVertexLabel(entry.group().name())) {
                    mgmt.makeVertexLabel(entry.group().name()).make();
                }
                if (!mgmt.containsPropertyKey(entry.name())) {
                    mgmt.makePropertyKey(entry.name()).dataType(String.class).cardinality(Cardinality.SINGLE).make();
                }
            }
            if (entry.type() == Type.PROPERTY){
                if (!mgmt.containsPropertyKey(entry.name())) {
                    mgmt.makePropertyKey(entry.name()).dataType(dataType(entry.extractor().name())).cardinality(Cardinality.SINGLE).make();
                }
            }

        }
        mgmt.commit();
    }

    public static void createEdgeLabel(Collection<Args.Option<File[]>> files) {

        JanusGraphManagement mgmt = getGraph().openManagement();


        for (Args.Option<File[]> file: files){
            if (!mgmt.containsEdgeLabel(file.metadata())) {
                mgmt.makeEdgeLabel(file.metadata()).make();
            }
        }
        mgmt.commit();
    }

    public static void createVertexLabel(Collection<Args.Option<File[]>> files) {

        JanusGraphManagement mgmt = getGraph().openManagement();


        for (Args.Option<File[]> file: files){
            if (!mgmt.containsVertexLabel(file.metadata())) {
                mgmt.makeVertexLabel(file.metadata()).make();
            }
        }
        mgmt.commit();
    }

    private static Class<?> dataType(String name){

        if ("Float".equals(name) || "float".equals(name)){
            return Float.class;
        }
        if ("Double".equals(name) || "double".equals(name)){
            return Double.class;
        }
        if ("Long".equals(name) || "long".equals(name)){
            return Long.class;
        }
        if ("Int".equals(name) || "int".equals(name)){
            return Integer.class;
        }
        if ("Boolean".equals(name) || "boolean".equals(name)){
            return Boolean.class;
        }
        if ("Char".equals(name) || "char".equals(name)){
            return Character.class;
        }
        return String.class;
    }

}
