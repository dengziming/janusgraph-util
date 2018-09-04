package janusgraph.util.batchimport.unsafe.input.csv;


import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.input.InputEntity;
import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;
import org.janusgraph.core.PropertyKey;

import java.io.IOException;
import java.util.Arrays;

/**
 * An array of {@link InputEntity} looking like an {@link InputEntityVisitor} to be able to fit into thinks like {@link Decorator}.
 */
public class InputEntityArray implements InputEntityVisitor
{
    private final InputEntity[] entities;
    private int cursor;

    public InputEntityArray(int length )
    {
        this.entities = new InputEntity[length];
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public boolean propertyId( long nextProp )
    {
        return currentEntity().propertyId( nextProp );
    }

    @Override
    public boolean property( String key, Object value )
    {
        return currentEntity().property( key, value );
    }

    @Override
    public boolean property(PropertyKey key, Object value) {
        return currentEntity().property( key, value );
    }

    @Override
    public boolean property( int propertyKeyId, Object value )
    {
        return currentEntity().property( propertyKeyId, value );
    }

    @Override
    public boolean id( long id )
    {
        return currentEntity().id( id );
    }

    @Override
    public boolean id( Object id, Group group )
    {
        return currentEntity().id( id, group );
    }

    @Override
    public boolean labels( String[] labels )
    {
        return currentEntity().labels( labels );
    }

    @Override
    public boolean labelField( long labelField )
    {
        return currentEntity().labelField( labelField );
    }

    @Override
    public boolean startId( long id )
    {
        return currentEntity().startId( id );
    }

    @Override
    public boolean startId( Object id, Group group )
    {
        return currentEntity().startId( id, group );
    }

    @Override
    public boolean endId( long id )
    {
        return currentEntity().endId( id );
    }

    @Override
    public boolean endId( Object id, Group group )
    {
        return currentEntity().endId( id, group );
    }

    @Override
    public boolean type(long type) {
        return currentEntity().type( type );
    }

    @Override
    public boolean type( String type )
    {
        return currentEntity().type( type );
    }

    @Override
    public void endOfEntity() throws IOException
    {
        currentEntity().endOfEntity();
        cursor++;
    }

    private InputEntity currentEntity()
    {
        if ( entities[cursor] == null )
        {
            entities[cursor] = new InputEntity();
        }
        return entities[cursor];
    }

    public InputEntity[] toArray()
    {
        return cursor == entities.length ? entities : Arrays.copyOf( entities, cursor );
    }
}
