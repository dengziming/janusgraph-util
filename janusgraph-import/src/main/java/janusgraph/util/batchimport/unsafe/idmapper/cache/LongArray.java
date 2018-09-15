package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Abstraction of a {@code long[]} so that different implementations can be plugged in, for example
 * off-heap, dynamically growing, or other implementations.
 *
 * @see NumberArrayFactory
 */
public interface LongArray extends NumberArray<LongArray>
{
    long get(long index);

    void set(long index, long value);

    @Override
    default void swap(long fromIndex, long toIndex)
    {
        long intermediary = get( fromIndex );
        set( fromIndex, get( toIndex ) );
        set( toIndex, intermediary );
    }
}