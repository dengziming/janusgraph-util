package janusgraph.util.batchimport.unsafe.idmapper.cache;


import janusgraph.util.batchimport.unsafe.helps.UnsafeUtil;

/**
 * Off-heap version of {@link LongArray} using {@code sun.misc.Unsafe}. Supports arrays with length beyond
 * Integer.MAX_VALUE.
 */
public class OffHeapLongArray extends OffHeapRegularNumberArray<LongArray> implements LongArray
{
    private final long defaultValue;

    public OffHeapLongArray(long length, long defaultValue, long base )
    {
        super( length, 3, base );
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public long get( long index )
    {
        return UnsafeUtil.getLong( addressOf( index ) );
    }

    @Override
    public void set( long index, long value )
    {
        UnsafeUtil.putLong( addressOf( index ), value );
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
                UnsafeUtil.putLong( adr, defaultValue );
            }
        }
    }
}
