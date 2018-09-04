package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Base class for common functionality for any {@link NumberArray} where the data lives off-heap.
 */
abstract class OffHeapRegularNumberArray<N extends NumberArray<N>> extends OffHeapNumberArray<N>
{
    protected final int shift;

    protected OffHeapRegularNumberArray( long length, int shift, long base )
    {
        super( length, 1 << shift, base );
        this.shift = shift;
    }

    protected long addressOf( long index )
    {
        index = rebase( index );
        if ( index < 0 || index >= length )
        {
            throw new ArrayIndexOutOfBoundsException( "Requested index " + index + ", but length is " + length );
        }
        return address + (index << shift);
    }

    protected boolean isByteUniform( long value )
    {
        byte any = (byte)value;
        for ( int i = 1; i < itemSize; i++ )
        {
            byte test = (byte)(value >>> (i << 3));
            if ( test != any )
            {
                return false;
            }
        }
        return true;
    }
}
