package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Contains basic functionality of fixed size number arrays.
 */
abstract class BaseNumberArray<N extends NumberArray<N>> implements NumberArray<N>
{
    protected final int itemSize;
    protected final long base;

    /**
     * @param itemSize byte size of each item in this array.
     * @param base base index to rebase all indexes in accessor methods off of. See {@link #at(long)}.
     */
    protected BaseNumberArray(int itemSize, long base )
    {
        this.itemSize = itemSize;
        this.base = base;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public N at( long index )
    {
        return (N)this;
    }

    /**
     * Utility for rebasing an external index to internal index.
     * @param index external index.
     * @return index into internal data structure.
     */
    protected long rebase( long index )
    {
        return index - base;
    }
}
