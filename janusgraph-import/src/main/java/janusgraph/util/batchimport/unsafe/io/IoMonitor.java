package janusgraph.util.batchimport.unsafe.io;


import janusgraph.util.batchimport.unsafe.stats.Key;
import janusgraph.util.batchimport.unsafe.stats.Keys;
import janusgraph.util.batchimport.unsafe.stats.Stat;
import janusgraph.util.batchimport.unsafe.stats.StatsProvider;

import static java.lang.System.currentTimeMillis;

/**
 * {@link IoTracer} exposed as a {@link StatsProvider}.
 *
 * Assumes that I/O is busy all the time.
 */
public class IoMonitor implements StatsProvider
{
    private volatile long startTime;
    private volatile long endTime;
    private final IoTracer tracer;
    private long resetPoint;

    public IoMonitor( IoTracer tracer )
    {
        this.tracer = tracer;
        reset();
    }

    public void reset()
    {
        startTime = currentTimeMillis();
        endTime = 0;
        resetPoint = tracer.countBytesWritten();
    }

    public void stop()
    {
        endTime = currentTimeMillis();
    }

    public long startTime()
    {
        return startTime;
    }

    public long totalBytesWritten()
    {
        return tracer.countBytesWritten() - resetPoint;
    }

    @Override
    public Stat stat(Key key )
    {
        if ( key == Keys.io_throughput )
        {
            return new IoThroughputStat( startTime, endTime, totalBytesWritten() );
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        return new Key[] { Keys.io_throughput };
    }
}
