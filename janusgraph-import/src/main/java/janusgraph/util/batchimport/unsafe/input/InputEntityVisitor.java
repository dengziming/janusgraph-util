package janusgraph.util.batchimport.unsafe.input;

import org.janusgraph.core.PropertyKey;

import java.io.Closeable;
import java.io.IOException;

/**
 * Receives calls for extracted data from {@link InputChunk}. This callback design allows for specific methods
 * using primitives and other optimizations, to avoid garbage.
 */
public interface InputEntityVisitor extends Closeable
{
    boolean propertyId(long nextProp);

    boolean property(String key, Object value);

    boolean property(PropertyKey key, Object value);

    boolean property(int propertyKeyId, Object value);

//    boolean edge(JanusGraphVertex other, EdgeLabel edgeLabel);
//
//    boolean edge(long other, EdgeLabel edgeLabel);

    // For nodes
    boolean id(long id);

    boolean id(Object id, Group group);

    boolean labels(String[] labels);

    boolean labelField(long labelField);

    // For relationships
    boolean startId(long id);

    boolean startId(Object id, Group group);

    boolean endId(long id);

    boolean endId(Object id, Group group);

    boolean type(long type);

    boolean type(String type);

    void endOfEntity() throws IOException;

    class Adapter implements InputEntityVisitor
    {
        @Override
        public boolean property( String key, Object value )
        {
            return true;
        }

        @Override
        public boolean property(PropertyKey key, Object value) {
            return true;
        }

        @Override
        public boolean property( int propertyKeyId, Object value )
        {
            return true;
        }

        /*@Override
        public boolean edge(JanusGraphVertex other, EdgeLabel edgeLabel) {
            return true;
        }

        @Override
        public boolean edge(long other, EdgeLabel edgeLabel) {
            return true;
        }*/

        @Override
        public boolean propertyId( long nextProp )
        {
            return true;
        }

        @Override
        public boolean id( long id )
        {
            return true;
        }

        @Override
        public boolean id( Object id, Group group )
        {
            return true;
        }

        @Override
        public boolean labels( String[] labels )
        {
            return true;
        }

        @Override
        public boolean startId( long id )
        {
            return true;
        }

        @Override
        public boolean startId( Object id, Group group )
        {
            return true;
        }

        @Override
        public boolean endId( long id )
        {
            return true;
        }

        @Override
        public boolean endId( Object id, Group group )
        {
            return true;
        }

        @Override
        public boolean type( long type )
        {
            return true;
        }

        @Override
        public boolean type( String type )
        {
            return true;
        }

        @Override
        public boolean labelField( long labelField )
        {
            return true;
        }

        @Override
        public void endOfEntity()
        {
        }

        @Override
        public void close() throws IOException
        {
        }
    }

    class Delegate implements InputEntityVisitor
    {
        private final InputEntityVisitor actual;

        public Delegate( InputEntityVisitor actual )
        {
            this.actual = actual;
        }

        @Override
        public boolean propertyId( long nextProp )
        {
            return actual.propertyId( nextProp );
        }

        @Override
        public boolean property( String key, Object value )
        {
            return actual.property( key, value );
        }

        @Override
        public boolean property(PropertyKey key, Object value) {
            return actual.property( key, value );
        }

        @Override
        public boolean property( int propertyKeyId, Object value )
        {
            return actual.property( propertyKeyId, value );
        }

        /*@Override
        public boolean edge(JanusGraphVertex other, EdgeLabel edgeLabel) {
            return actual.edge(other,edgeLabel);
        }

        @Override
        public boolean edge(long other, EdgeLabel edgeLabel) {
            return actual.edge(other,edgeLabel);
        }*/

        @Override
        public boolean id( long id )
        {
            return actual.id( id );
        }

        @Override
        public boolean id( Object id, Group group )
        {
            return actual.id( id, group );
        }

        @Override
        public boolean labels( String[] labels )
        {
            return actual.labels( labels );
        }

        @Override
        public boolean labelField( long labelField )
        {
            return actual.labelField( labelField );
        }

        @Override
        public boolean startId( long id )
        {
            return actual.startId( id );
        }

        @Override
        public boolean startId( Object id, Group group )
        {
            return actual.startId( id, group );
        }

        @Override
        public boolean endId( long id )
        {
            return actual.endId( id );
        }

        @Override
        public boolean endId( Object id, Group group )
        {
            return actual.endId( id, group );
        }

        @Override
        public boolean type( long type )
        {
            return actual.type( type );
        }

        @Override
        public boolean type( String type )
        {
            return actual.type( type );
        }

        @Override
        public void endOfEntity() throws IOException
        {
            actual.endOfEntity();
        }

        @Override
        public void close() throws IOException
        {
            actual.close();
        }
    }

    InputEntityVisitor NULL = new Adapter()
    {   // empty
    };
}
