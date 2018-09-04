package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;

/**
 * Factory for {@link Tracker} instances.
 */
public interface TrackerFactory
{
    /**
     * @param arrayFactory {@link NumberArrayFactory} to use as backing data structure for the {@link Tracker}.
     * @param size size of the tracker.
     * @return {@link Tracker} capable of keeping track of {@code size} items.
     */
    Tracker create(NumberArrayFactory arrayFactory, long size);
}
