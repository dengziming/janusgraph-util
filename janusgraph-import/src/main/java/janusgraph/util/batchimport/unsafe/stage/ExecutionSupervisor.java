package janusgraph.util.batchimport.unsafe.stage;


import java.time.Clock;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Thread.sleep;

/**
 * Supervises a {@link StageExecution} until it is no longer {@link StageExecution#stillExecuting() executing}.
 * Meanwhile it feeds information about the execution to an {@link ExecutionMonitor}.
 */
public class ExecutionSupervisor
{
    private final Clock clock;
    private final ExecutionMonitor monitor;

    public ExecutionSupervisor( Clock clock, ExecutionMonitor monitor )
    {
        this.clock = clock;
        this.monitor = monitor;
    }

    public ExecutionSupervisor( ExecutionMonitor monitor )
    {
        this( Clocks.systemClock(), monitor );
    }

    /**
     * Supervises {@link StageExecution}, provides continuous information to the {@link ExecutionMonitor}
     * and returns when the execution is done or an error occurs, in which case an exception is thrown.
     *
     * Made synchronized to ensure that only one set of executions take place at any given time
     * and also to make sure the calling thread goes through a memory barrier (useful both before and after execution).
     *
     * @param execution {@link StageExecution} instances to supervise simultaneously.
     */
    public synchronized void supervise( StageExecution execution )
    {
        long startTime = currentTimeMillis();
        start( execution );

        while ( execution.stillExecuting() )
        {
            finishAwareSleep( execution );
            monitor.check( execution );
        }
        end( execution, currentTimeMillis() - startTime );
    }

    private long currentTimeMillis()
    {
        return clock.millis();
    }

    protected void end( StageExecution execution, long totalTimeMillis )
    {
        monitor.end( execution, totalTimeMillis );
    }

    protected void start( StageExecution execution )
    {
        monitor.start( execution );
    }

    private void finishAwareSleep( StageExecution execution )
    {
        long endTime = monitor.nextCheckTime();
        while ( currentTimeMillis() < endTime )
        {
            if ( !execution.stillExecuting() )
            {
                break;
            }

            try
            {
                sleep( min( 10, max( 0, endTime - currentTimeMillis() ) ) );
            }
            catch ( InterruptedException e )
            {
                execution.panic( e );
                break;
            }
        }
    }
}
