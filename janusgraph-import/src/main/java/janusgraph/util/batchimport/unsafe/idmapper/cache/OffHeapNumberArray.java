package janusgraph.util.batchimport.unsafe.idmapper.cache;


import janusgraph.util.batchimport.unsafe.helps.UnsafeUtil;

public abstract class OffHeapNumberArray<N extends NumberArray<N>> extends BaseNumberArray<N>
{
    private final long allocatedAddress;
    protected final long address;
    protected final long length;
    private boolean closed;

    protected OffHeapNumberArray( long length, int itemSize, long base )
    {
        super( itemSize, base );
        UnsafeUtil.assertHasUnsafe();
        this.length = length;

        long dataSize = length * itemSize;
        boolean itemSizeIsPowerOfTwo = Integer.bitCount( itemSize ) == 1;
        if ( UnsafeUtil.allowUnalignedMemoryAccess || !itemSizeIsPowerOfTwo )
        {
            // we can end up here even if we require aligned memory access. Reason is that item size
            // isn't power of two anyway and so we have to fallback to safer means of accessing the memory,
            // i.e. byte for byte.
            this.allocatedAddress = this.address = UnsafeUtil.allocateMemory( dataSize );
        }
        else
        {
            // the item size is a power of two and we're required to access memory aligned
            // so we can allocate a bit more to ensure we can get an aligned memory address to start from.
            this.allocatedAddress = UnsafeUtil.allocateMemory( dataSize + itemSize - 1 );
            this.address = UnsafeUtil.alignedMemory( allocatedAddress, itemSize );
        }
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( length * itemSize );
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            if ( length > 0 )
            {
                // Allocating 0 bytes actually returns address 0
                UnsafeUtil.free( allocatedAddress );
            }
            closed = true;
        }
    }
}
