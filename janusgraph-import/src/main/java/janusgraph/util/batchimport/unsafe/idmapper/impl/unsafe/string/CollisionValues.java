package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;

/**
 * Stores collision values efficiently for retrieval later. The idea is that there's a single thread {@link #add(Object) adding}
 * ids, each gets assigned an offset, and later use those offsets to get back the added ids.
 */
public interface CollisionValues extends MemoryStatsVisitor.Visitable, AutoCloseable
{
    long add(Object id);

    Object get(long offset);

    @Override
    void close();
}
