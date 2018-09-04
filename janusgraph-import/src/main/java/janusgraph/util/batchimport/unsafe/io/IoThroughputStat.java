package janusgraph.util.batchimport.unsafe.io;


import janusgraph.util.batchimport.unsafe.helps.Format;
import janusgraph.util.batchimport.unsafe.stats.DetailLevel;
import janusgraph.util.batchimport.unsafe.stats.Stat;

import static java.lang.System.currentTimeMillis;

/**
 * {@link Stat} that provides a simple Mb/s stat, mostly used for getting an insight into I/O throughput.
 */
public class IoThroughputStat implements Stat
{
    private final long startTime;
    private final long endTime;
    private final long position;

    public IoThroughputStat(long startTime, long endTime, long position )
    {
        this.startTime = startTime;
        this.endTime = endTime;
        this.position = position;
    }

    @Override
    public DetailLevel detailLevel()
    {
        return DetailLevel.IMPORTANT;
    }

    @Override
    public long asLong()
    {
        long endTime = this.endTime != 0 ? this.endTime : currentTimeMillis();
        long totalTime = endTime - startTime;
        int seconds = (int) (totalTime / 1000);
        return seconds > 0 ? position / seconds : -1;
    }

    @Override
    public String toString()
    {
        long stat = asLong();
        return stat == -1 ? "??" : Format.bytes( stat ) + "/s";
    }
}
