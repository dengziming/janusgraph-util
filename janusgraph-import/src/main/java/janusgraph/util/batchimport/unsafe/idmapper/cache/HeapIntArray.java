package janusgraph.util.batchimport.unsafe.idmapper.cache;

import java.util.Arrays;

/**
 * A {@code long[]} on heap, abstracted into a {@link IntArray}.
 */
public class HeapIntArray extends HeapNumberArray<IntArray> implements IntArray
{
    private final int[] array;
    private final int defaultValue;

    public HeapIntArray( int length, int defaultValue, long base )
    {
        super( 4, base );
        this.defaultValue = defaultValue;
        this.array = new int[length];
        clear();
    }

    @Override
    public long length()
    {
        return array.length;
    }

    @Override
    public int get( long index )
    {
        return array[index( index )];
    }

    @Override
    public void set( long index, int value )
    {
        array[index( index )] = value;
    }

    @Override
    public void clear()
    {
        Arrays.fill( array, defaultValue );
    }
}
