package janusgraph.util.batchimport.unsafe;


import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.stage.GatheringMemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.stats.DetailLevel;
import janusgraph.util.batchimport.unsafe.stats.GenericStatsProvider;
import janusgraph.util.batchimport.unsafe.stats.Keys;
import janusgraph.util.batchimport.unsafe.stats.Stat;

import static janusgraph.util.batchimport.unsafe.helps.Format.bytes;

/**
 * Provides {@link Stat statistics} about memory usage, as the key {@link Keys#memory_usage}
 */
public class MemoryUsageStatsProvider extends GenericStatsProvider implements Stat
{
    private final MemoryStatsVisitor.Visitable[] users;

    public MemoryUsageStatsProvider(MemoryStatsVisitor.Visitable... users )
    {
        this.users = users;
        add( Keys.memory_usage, this );
    }

    @Override
    public DetailLevel detailLevel()
    {
        return DetailLevel.IMPORTANT;
    }

    @Override
    public long asLong()
    {
        GatheringMemoryStatsVisitor visitor = new GatheringMemoryStatsVisitor();
        for ( MemoryStatsVisitor.Visitable user : users )
        {
            user.acceptMemoryStatsVisitor( visitor );
        }
        return visitor.getHeapUsage() + visitor.getOffHeapUsage();
    }

    @Override
    public String toString()
    {
        return bytes( asLong() );
    }
}
