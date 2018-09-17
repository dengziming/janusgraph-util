package janusgraph.util.batchimport.unsafe.idmapper;


import janusgraph.util.batchimport.unsafe.helps.collection.PrimitiveLongCollections;
import janusgraph.util.batchimport.unsafe.helps.collection.PrimitiveLongIterator;
import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.idmapper.impl.StringEncoder;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.EncodingIdMapper;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.StringCollisionValues;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.raddix.Radix;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.input.Groups;
import janusgraph.util.batchimport.unsafe.progress.ProgressListener;

import java.util.function.LongFunction;

import static janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.TrackerFactories.dynamic;


/**
 * Place to instantiate common {@link IdMapper} implementations.
 */
public class IdMappers
{
    private static class ActualIdMapper implements IdMapper<Long>
    {
        @Override
        public void put(Long inputId, Group group , long actualId)
        {   // No need to remember anything
        }

        @Override
        public boolean needsPreparation()
        {
            return false;
        }

        @Override
        public void prepare(LongFunction<Long> inputIdLookup, Collector collector, ProgressListener progress )
        {   // No need to prepare anything
        }

        @Override
        public long get( Long inputId ,Group group )
        {
            return ((Long)inputId).longValue();
        }

        @Override
        public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
        {   // No memory usage
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }

        @Override
        public void close()
        {   // Nothing to close
        }

        @Override
        public long calculateMemoryUsage( long numberOfNodes )
        {
            return 0;
        }

        @Override
        public PrimitiveLongIterator leftOverDuplicateNodesIds()
        {
            return PrimitiveLongCollections.emptyIterator();
        }
    }

    private IdMappers()
    {
    }

    /**
     * An {@link IdMapper} that doesn't touch the input ids, but just asserts that node ids arrive in ascending order.
     * This is for advanced usage and puts constraints on the input in that all node ids given as input
     * must be valid. There will not be further checks, other than that for order of the ids.
     */
    public static IdMapper actual()
    {
        return new ActualIdMapper();
    }

    public static final EncodingIdMapper.Monitor NO_MONITOR = count ->
    {   // Do nothing.
    };
    /**
     * An {@link IdMapper} capable of mapping {@link String strings} to long ids.
     *
     * @param cacheFactory {@link NumberArrayFactory} for allocating memory for the cache used by this index.
     * @param groups {@link Groups} containing all id groups.
     * @return {@link IdMapper} for when input ids are strings.
     */
    public static IdMapper<String> strings(NumberArrayFactory cacheFactory, Groups groups )
    {
        return new EncodingIdMapper( cacheFactory, new StringEncoder(), Radix.STRING, NO_MONITOR, dynamic(), groups,
                numberOfCollisions -> new StringCollisionValues( cacheFactory, numberOfCollisions ) );
    }

}
