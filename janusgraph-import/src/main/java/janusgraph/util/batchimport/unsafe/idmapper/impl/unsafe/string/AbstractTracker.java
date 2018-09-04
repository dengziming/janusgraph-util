package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArray;

/**
 * Base implementation of {@link Tracker} over a {@link NumberArray}.
 *
 * @param <ARRAY> type of {@link NumberArray} in this implementation.
 */
abstract class AbstractTracker<ARRAY extends NumberArray> implements Tracker
{
    protected ARRAY array;

    protected AbstractTracker( ARRAY array )
    {
        this.array = array;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        array.acceptMemoryStatsVisitor( visitor );
    }

    @Override
    public void swap( long fromIndex, long toIndex )
    {
        array.swap( fromIndex, toIndex );
    }

    @Override
    public void close()
    {
        array.close();
    }
}
