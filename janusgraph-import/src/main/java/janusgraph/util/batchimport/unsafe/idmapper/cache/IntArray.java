package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Abstraction of a {@code int[]} so that different implementations can be plugged in, for example
 * off-heap, dynamically growing, or other implementations.
 *
 * @see NumberArrayFactory
 */
public interface IntArray extends NumberArray<IntArray>
{
    int get(long index);

    void set(long index, int value);

    @Override
    default void swap(long fromIndex, long toIndex)
    {
        int intermediary = get( fromIndex );
        set( fromIndex, get( toIndex ) );
        set( toIndex, intermediary );
    }
}
