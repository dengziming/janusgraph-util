package janusgraph.util.batchimport.unsafe.progress;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

final class Aggregator
{
    private final Map<ProgressListener, ProgressListener.MultiPartProgressListener.State> states = new HashMap<>();
    private final Indicator indicator;
    @SuppressWarnings( "unused"/*accessed through updater*/ )
    private volatile long progress;
    @SuppressWarnings( "unused"/*accessed through updater*/ )
    private volatile int last;
    private static final AtomicLongFieldUpdater<Aggregator> PROGRESS = newUpdater( Aggregator.class, "progress" );
    private static final AtomicIntegerFieldUpdater<Aggregator> LAST =
            AtomicIntegerFieldUpdater.newUpdater( Aggregator.class, "last" );
    private long totalCount;

    Aggregator( Indicator indicator )
    {
        this.indicator = indicator;
    }

    synchronized void add( ProgressListener progress, long totalCount )
    {
        states.put( progress, ProgressListener.MultiPartProgressListener.State.INIT );
        this.totalCount += totalCount;
    }

    synchronized void initialize()
    {
        indicator.startProcess( totalCount );
        if ( states.isEmpty() )
        {
            indicator.progress( 0, indicator.reportResolution() );
            indicator.completeProcess();
        }
    }

    void update( long delta )
    {
        long progress = PROGRESS.addAndGet( this, delta );
        int current = (int) ((progress * indicator.reportResolution()) / totalCount);
        for ( int last = this.last; current > last; last = this.last )
        {
            if ( LAST.compareAndSet( this, last, current ) )
            {
                synchronized ( this )
                {
                    indicator.progress( last, current );
                }
            }
        }
    }

    synchronized void start( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.put( part, ProgressListener.MultiPartProgressListener.State.LIVE ) == ProgressListener.MultiPartProgressListener.State.INIT )
        {
            indicator.startPart( part.part, part.totalCount );
        }
    }

    synchronized void complete( ProgressListener.MultiPartProgressListener part )
    {
        if ( states.remove( part ) != null )
        {
            indicator.completePart( part.part );
            if ( states.isEmpty() )
            {
                indicator.completeProcess();
            }
        }
    }

    synchronized void signalFailure( Throwable e )
    {
        indicator.failure( e );
    }
}
