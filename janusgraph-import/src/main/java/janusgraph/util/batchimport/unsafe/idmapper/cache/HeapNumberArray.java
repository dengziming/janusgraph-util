package janusgraph.util.batchimport.unsafe.idmapper.cache;

import static janusgraph.util.batchimport.unsafe.helps.Numbers.safeCastLongToInt;

/**
 * Base class for common functionality for any {@link NumberArray} where the data lives inside heap.
 */
abstract class HeapNumberArray<N extends NumberArray<N>> extends BaseNumberArray<N>
{
    protected HeapNumberArray( int itemSize, long base )
    {
        super( itemSize, base );
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.heapUsage( length() * itemSize ); // roughly
    }

    @Override
    public void close()
    {   // Nothing to close
    }

    protected int index( long index )
    {
        return safeCastLongToInt( rebase( index ) );
    }


}
