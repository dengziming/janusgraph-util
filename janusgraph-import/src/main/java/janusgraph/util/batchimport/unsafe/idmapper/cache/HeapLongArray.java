package janusgraph.util.batchimport.unsafe.idmapper.cache;

import java.util.Arrays;

/**
 * A {@code long[]} on heap, abstracted into a {@link LongArray}.
 */
public class HeapLongArray extends HeapNumberArray<LongArray> implements LongArray
{
    private final long[] array;
    private final long defaultValue;

    public HeapLongArray( int length, long defaultValue, long base )
    {
        super( 8, base );
        this.defaultValue = defaultValue;
        this.array = new long[length];
        clear();
    }

    @Override
    public long length()
    {
        return array.length;
    }

    @Override
    public long get( long index )
    {
        return array[index( index )];
    }

    @Override
    public void set( long index, long value )
    {
        array[index( index )] = value;
    }

    @Override
    public void clear()
    {
        Arrays.fill( array, defaultValue );
    }
}
