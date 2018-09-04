package janusgraph.util.batchimport.unsafe.stats;

/**
 * Common {@link Stat} implementations.
 */
public class Stats
{
    public abstract static class LongBasedStat implements Stat
    {
        private final DetailLevel detailLevel;

        public LongBasedStat( DetailLevel detailLevel )
        {
            this.detailLevel = detailLevel;
        }

        @Override
        public DetailLevel detailLevel()
        {
            return detailLevel;
        }

        @Override
        public String toString()
        {
            return String.valueOf( asLong() );
        }
    }

    private Stats()
    {
    }

    public static Stat longStat( final long stat )
    {
        return longStat( stat, DetailLevel.BASIC );
    }

    public static Stat longStat( final long stat, DetailLevel detailLevel )
    {
        return new LongBasedStat( detailLevel )
        {
            @Override
            public long asLong()
            {
                return stat;
            }
        };
    }
}
