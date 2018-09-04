package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Visits objects able to provide stats about amount of used memory.
 */
public interface MemoryStatsVisitor
{
    interface Visitable
    {
        void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor);
    }

    void heapUsage(long bytes);

    void offHeapUsage(long bytes);
}
