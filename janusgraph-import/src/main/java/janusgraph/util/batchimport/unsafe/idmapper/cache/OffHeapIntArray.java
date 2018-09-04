package janusgraph.util.batchimport.unsafe.idmapper.cache;


import janusgraph.util.batchimport.unsafe.helps.UnsafeUtil;

/**
 * Off-heap version of {@link IntArray} using {@code sun.misc.Unsafe}. Supports arrays with length beyond
 * Integer.MAX_VALUE.
 */
public class OffHeapIntArray extends OffHeapRegularNumberArray<IntArray> implements IntArray
{
    private final int defaultValue;

    public OffHeapIntArray( long length, int defaultValue, long base )
    {
        super( length, 2, base );
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public int get( long index )
    {
        return UnsafeUtil.getInt( addressOf( index ) );
    }

    @Override
    public void set( long index, int value )
    {
        UnsafeUtil.putInt( addressOf( index ), value );
    }

    @Override
    public void clear()
    {
        if ( isByteUniform( defaultValue ) )
        {
            UnsafeUtil.setMemory( address, length << shift, (byte)defaultValue );
        }
        else
        {
            for ( long i = 0, adr = address; i < length; i++, adr += itemSize )
            {
                UnsafeUtil.putInt( adr, defaultValue );
            }
        }
    }
}
