package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;

import java.util.function.LongFunction;

/**
 * {@link EncodingIdMapper} is an index where arbitrary ids, be it {@link String} or {@code long} or whatever
 * can be added and mapped to an internal (node) {@code long} id. The order in which ids are added can be
 * any order and so in the end when all ids have been added the index goes through a
 * {@link IDMapper#prepare(LongFunction, Collector, ProgressListener) prepare phase} where these ids are sorted
 * so that {@link IdMapper#get(Object, Group)} can execute efficiently later on.
 * <p>
 * In that sorting the ids aren't moved, but instead a {@link Tracker} created where these moves are recorded
 * and the initial data (in order of insertion) is kept intact to be able to track {@link Group} belonging among
 * other things. Since a tracker is instantiated after all ids have been added there's an opportunity to create
 * a smaller data structure for smaller datasets, for example those that fit inside {@code int} range.
 * That's why this abstraction exists so that the best suited implementation can be picked for every import.
 */
public interface Tracker extends MemoryStatsVisitor.Visitable, AutoCloseable
{
    /**
     * @param index data index to get the value for.
     * @return value previously {@link #set(long, long)}.
     */
    long get(long index);

    /**
     * Swaps values from {@code fromIndex} to {@code toIndex}.
     *
     * @param fromIndex index to swap from.
     * @param toIndex index to swap to.
     */
    void swap(long fromIndex, long toIndex);

    /**
     * Sets {@code value} at the specified {@code index}.
     *
     * @param index data index to set value at.
     * @param value value to set at that index.
     */
    void set(long index, long value);

    void markAsDuplicate(long index);

    boolean isMarkedAsDuplicate(long index);

    @Override
    void close();
}
