package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.helps.DependencyResolver;

import java.time.Clock;

/**
 * {@link ExecutionMonitor} that wraps several other monitors. Each wrapper monitor can still specify
 * individual poll frequencies and this {@link MultiExecutionMonitor} will make that happen.
 */
public class MultiExecutionMonitor implements ExecutionMonitor
{
    private final Clock clock;
    private final ExecutionMonitor[] monitors;
    private final long[] endTimes;

    public MultiExecutionMonitor(ExecutionMonitor... monitors )
    {
        this( Clocks.systemClock(), monitors );
    }

    public MultiExecutionMonitor(Clock clock, ExecutionMonitor... monitors )
    {
        this.clock = clock;
        this.monitors = monitors;
        this.endTimes = new long[monitors.length];
        fillEndTimes();
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.initialize( dependencyResolver );
        }
    }

    @Override
    public void start( StageExecution execution )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.start( execution );
        }
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.end( execution, totalTimeMillis );
        }
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.done( totalTimeMillis, additionalInformation );
        }
    }

    @Override
    public long nextCheckTime()
    {
        // Find the lowest of all end times
        long low = endTimes[0];
        for ( int i = 1; i < monitors.length; i++ )
        {
            long thisLow = endTimes[i];
            if ( thisLow < low )
            {
                low = thisLow;
            }
        }
        return low;
    }

    private void fillEndTimes()
    {
        for ( int i = 0; i < monitors.length; i++ )
        {
            endTimes[i] = monitors[i].nextCheckTime();
        }
    }

    @Override
    public void check( StageExecution execution )
    {
        long currentTimeMillis = clock.millis();
        for ( int i = 0; i < monitors.length; i++ )
        {
            if ( currentTimeMillis >= endTimes[i] )
            {
                monitors[i].check( execution );
                endTimes[i] = monitors[i].nextCheckTime();
            }
        }
    }
}
