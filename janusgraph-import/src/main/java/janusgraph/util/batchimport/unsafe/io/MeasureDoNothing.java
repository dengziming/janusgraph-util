package janusgraph.util.batchimport.unsafe.io;


import janusgraph.util.batchimport.unsafe.log.Log;

public class MeasureDoNothing extends Thread
{
    public interface Monitor
    {
        default void started()
        {
        }

        void blocked(long millis);

        default void stopped()
        {
        }
    }

    public static Monitor loggingMonitor( Log log )
    {
        return new Monitor()
        {
            @Override
            public void started()
            {
                log.debug( "GC Monitor started. " );
            }

            @Override
            public void stopped()
            {
                log.debug( "GC Monitor stopped. " );
            }

            @Override
            public void blocked( long millis )
            {
                log.warn( String.format( "GC Monitor: Application threads blocked for %dms.", millis ) );
            }
        };
    }

    public static class CollectingMonitor implements Monitor
    {
        private long gcBlockTime;

        @Override
        public void blocked( long millis )
        {
            gcBlockTime += millis;
        }

        public long getGcBlockTime()
        {
            return gcBlockTime;
        }
    }

    private volatile boolean measure = true;

    private final Monitor monitor;
    private final long TIME_TO_WAIT;
    private final long NOTIFICATION_THRESHOLD;

    public MeasureDoNothing(String threadName, Monitor monitor, long timeToWait, long pauseNotificationThreshold )
    {
        super( threadName );
        this.monitor = monitor;
        this.TIME_TO_WAIT = timeToWait;
        this.NOTIFICATION_THRESHOLD = pauseNotificationThreshold + timeToWait;
        setDaemon( true );
    }

    @Override
    public synchronized void run()
    {
        monitor.started();
        while ( measure )
        {
            long start = System.nanoTime();
            try
            {
                this.wait( TIME_TO_WAIT );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
            long time = (System.nanoTime() - start) / 1_000_000;
            if ( time > NOTIFICATION_THRESHOLD )
            {
                long blockTime = time - TIME_TO_WAIT;
                monitor.blocked( blockTime );
            }
        }
        monitor.stopped();
    }

    public synchronized void stopMeasuring()
    {
        measure = false;
        this.interrupt();
    }
}
