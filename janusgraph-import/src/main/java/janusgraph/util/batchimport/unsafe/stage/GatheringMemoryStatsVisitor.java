package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.helps.ByteUnit;

/**
 * {@link MemoryStatsVisitor} that can gather stats from multiple sources and give a total.
 */
public class GatheringMemoryStatsVisitor implements MemoryStatsVisitor
{
    private long heapUsage;
    private long offHeapUsage;

    @Override
    public void heapUsage( long bytes )
    {
        heapUsage += bytes;
    }

    @Override
    public void offHeapUsage( long bytes )
    {
        offHeapUsage += bytes;
    }

    public long getHeapUsage()
    {
        return heapUsage;
    }

    public long getOffHeapUsage()
    {
        return offHeapUsage;
    }

    public long getTotalUsage()
    {
        return heapUsage + offHeapUsage;
    }

    @Override
    public String toString()
    {
        return "Memory usage[heap:" + ByteUnit.bytes( heapUsage ) + ", off-heap:" + ByteUnit.bytes( offHeapUsage ) + "]";
    }

    public static long totalMemoryUsageOf( MemoryStatsVisitor.Visitable... memoryUsers )
    {
        GatheringMemoryStatsVisitor memoryVisitor = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable memoryUser : memoryUsers )
        {
            memoryUser.acceptMemoryStatsVisitor( memoryVisitor );
        }
        return memoryVisitor.getTotalUsage();
    }
}
