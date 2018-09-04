package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Created by dengziming on 15/08/2018.
 * ${Main}
 */
public interface NumberArray<N extends NumberArray<N>> extends AutoCloseable, MemoryStatsVisitor.Visitable{
    /**
     * @return length of the array, i.e. the capacity.
     */
    long length();

    /**
     * Swaps items from {@code fromIndex} to {@code toIndex}, such that
     * {@code fromIndex} and {@code toIndex}, {@code fromIndex+1} and {@code toIndex} a.s.o swaps places.
     * The number of items swapped is equal to the length of the default value of the array.
     *  @param fromIndex where to start swapping from.
     * @param toIndex where to start swapping to.
     */
    void swap(long fromIndex, long toIndex);

    /**
     * Sets all values to a default value.
     */
    void clear();

    /**
     * Releases any resources that GC won't release automatically.
     */
    @Override
    void close();

    /**
     * Part of the nature of {@link NumberArray} is that {@link #length()} can be dynamically growing.
     * For that to work some implementations (those coming from e.g
     * {@link NumberArrayFactory#newDynamicIntArray(long, int)} and such dynamic calls) has an indirection,
     * one that is a bit costly when comparing to raw array access. In scenarios where there will be two or
     * more access to the same index in the array it will be more efficient to resolve this indirection once
     * and return the "raw" array for that given index so that it can be used directly in multiple calls,
     * knowing that the returned array holds the given index.
     *
     * @param index index into the array which the returned array will contain.
     * @return array sure to hold the given index.
     */
    N at(long index);
}
