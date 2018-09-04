package janusgraph.util.batchimport.unsafe.graph.serializer;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.Arrays;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 * serialize {@link org.janusgraph.core.PropertyKey propertykey} and {@link org.janusgraph.core.EdgeLabel edgeLabel} to
 * {@link org.janusgraph.diskstorage.Entry Entry} to be persist to database
 */
@Deprecated
public class RelationSerializer {

    private StandardJanusGraph graph;
    private static final int DEFAULT_CAPACITY = 128;



    public RelationSerializer(StandardJanusGraph graph) {
        this.graph = graph;
    }



    public StaticArrayEntry writeRelation(InternalRelation relation, InternalRelationType type, int position, StandardJanusGraphTx tx){


        long time1 = System.currentTimeMillis();
        assert type==relation.getType() || (type.getBaseType() != null && type.getBaseType().equals(relation.getType()));

        Direction dir = EdgeDirection.fromPosition(position);
        // Preconditions.checkArgument(type.isUnidirected(Direction.BOTH) || type.isUnidirected(dir));
        long typeid = type.longId();
        IDHandler.DirectionID dirID = getDirID(dir, relation.isProperty() ? RelationCategory.PROPERTY : RelationCategory.EDGE);

        DataOutput out = graph.getDataSerializer().getDataOutput(DEFAULT_CAPACITY);

        int valuePosition;
        IDHandler.writeRelationType(out, typeid, dirID, type.isInvisibleType());
        Multiplicity multiplicity = type.multiplicity();


        long relationId = relation.longId();

        long time2 = System.currentTimeMillis();

        //How multiplicity is handled for edges and properties is slightly different
        if (relation.isEdge()) {
            long start = System.currentTimeMillis();
            long otherVertexId = relation.getVertex((position + 1) % 2).longId();
            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) {
                    valuePosition = out.getPosition();
                    VariableLong.writePositive(out, otherVertexId);
                } else {
                    VariableLong.writePositiveBackward(out, otherVertexId);
                    valuePosition = out.getPosition();
                }
                VariableLong.writePositive(out, relationId);
            } else {
                VariableLong.writePositiveBackward(out, otherVertexId);
                VariableLong.writePositiveBackward(out, relationId);
                valuePosition = out.getPosition();
            }
            long end = System.currentTimeMillis();
        } else {
            long start = System.currentTimeMillis();
            assert relation.isProperty();
            Preconditions.checkArgument(relation.isProperty());
            Object value = ((JanusGraphVertexProperty) relation).value();
            Preconditions.checkNotNull(value);
            PropertyKey key = (PropertyKey) type;
            assert key.dataType().isInstance(value);

            if (multiplicity.isConstrained()) {
                if (multiplicity.isUnique(dir)) { //Cardinality=SINGLE
                    valuePosition = out.getPosition();
                    writePropertyValue(out,key,value);
                } else { //Cardinality=SET
                    writePropertyValue(out,key,value);
                    valuePosition = out.getPosition();
                }
                VariableLong.writePositive(out, relationId);
            } else {
                assert multiplicity.getCardinality()== Cardinality.LIST;
                VariableLong.writePositiveBackward(out, relationId);
                valuePosition = out.getPosition();
                writePropertyValue(out,key,value);
            }
            long end = System.currentTimeMillis();
        }

        long time3 = System.currentTimeMillis();
        //Write signature
        long[] signature = type.getSignature();
        writeInlineTypes(signature, relation, out, tx, InlineType.SIGNATURE);

        //Write remaining properties
        LongSet writtenTypes = new LongHashSet(signature.length);
        if (signature.length > 0) {
            for (long id : signature) writtenTypes.add(id);
        }
        LongArrayList remainingTypes = new LongArrayList(8);
        for (PropertyKey t : relation.getPropertyKeysDirect()) {
            if (!(t instanceof ImplicitKey) && !writtenTypes.contains(t.longId())) {
                remainingTypes.add(t.longId());
            }
        }
        //Sort types before writing to ensure that value is always written the same way
        long[] remaining = remainingTypes.toArray();
        Arrays.sort(remaining);
        for (long tid : remaining) {
            PropertyKey t = tx.getExistingPropertyKey(tid);
            writeInline(out, t, relation.getValueDirect(t), InlineType.NORMAL);
        }
        assert valuePosition>0;
        long time4 = System.currentTimeMillis();


        return new StaticArrayEntry(out.getStaticBuffer(),valuePosition);
    }

    private IDHandler.DirectionID getDirID(Direction dir, RelationCategory rt) {
        switch (rt) {
            case PROPERTY:
                assert dir == Direction.OUT;
                return IDHandler.DirectionID.PROPERTY_DIR;

            case EDGE:
                switch (dir) {
                    case OUT:
                        return IDHandler.DirectionID.EDGE_OUT_DIR;

                    case IN:
                        return IDHandler.DirectionID.EDGE_IN_DIR;

                    default:
                        throw new IllegalArgumentException("Invalid direction: " + dir);
                }

            default:
                throw new IllegalArgumentException("Invalid relation type: " + rt);
        }
    }
    private void writeInlineTypes(long[] keyIds,
                                         InternalRelation relation,
                                         DataOutput out,
                                         TypeInspector tx,
                                         InlineType inlineType) {
        for (long keyId : keyIds) {
            PropertyKey t = tx.getExistingPropertyKey(keyId);
            writeInline(out, t, relation.getValueDirect(t), inlineType);
        }
    }

    private void writeInline(DataOutput out, PropertyKey inlineKey, Object value, InlineType inlineType) {

        assert inlineType.writeInlineKey() || !AttributeUtil.hasGenericDataType(inlineKey);

        if (inlineType.writeInlineKey()) {
            IDHandler.writeInlineRelationType(out, inlineKey.longId());
        }

        writePropertyValue(out,inlineKey,value, inlineType);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value) {
        writePropertyValue(out,key,value, InlineType.NORMAL);
    }

    private void writePropertyValue(DataOutput out, PropertyKey key, Object value, InlineType inlineType) {
        if (AttributeUtil.hasGenericDataType(key)) {
            assert !inlineType.writeByteOrdered();
            out.writeClassAndObject(value);
        } else {
            assert value==null || value.getClass().equals(key.dataType());
            if (inlineType.writeByteOrdered()) out.writeObjectByteOrder(value, key.dataType());
            else out.writeObject(value, key.dataType());
        }
    }
    
    private enum InlineType {

        KEY, SIGNATURE, NORMAL;

        public boolean writeInlineKey() {
            return this==NORMAL;
        }

        public boolean writeByteOrdered() {
            return this==KEY;
        }

    }
}
