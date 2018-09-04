package janusgraph.util.batchimport.unsafe.stage;

import java.io.InputStream;

import static janusgraph.util.batchimport.unsafe.stage.HumanUnderstandableExecutionMonitor.NO_MONITOR;


/**
 * Common {@link ExecutionMonitor} implementations.
 */
public class ExecutionMonitors
{
    private ExecutionMonitors()
    {
        throw new AssertionError( "No instances allowed" );
    }

    public static ExecutionMonitor defaultVisible()
    {
        return defaultVisible( System.in );
    }

    public static ExecutionMonitor defaultVisible( InputStream in )
    {
        ProgressRestoringMonitor monitor = new ProgressRestoringMonitor();
        return new MultiExecutionMonitor(
                new HumanUnderstandableExecutionMonitor( System.out, NO_MONITOR, monitor ),
                new OnDemandDetailsExecutionMonitor( System.out, in, monitor ) );
    }

    private static final ExecutionMonitor INVISIBLE = new ExecutionMonitor()
    {
        @Override
        public void start( StageExecution execution )
        {   // Do nothing
        }

        @Override
        public void end( StageExecution execution, long totalTimeMillis )
        {   // Do nothing
        }

        @Override
        public long nextCheckTime()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public void check( StageExecution execution )
        {   // Do nothing
        }

        @Override
        public void done( long totalTimeMillis, String additionalInformation )
        {   // Do nothing
        }
    };

    public static ExecutionMonitor invisible()
    {
        return INVISIBLE;
    }
}
