package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.helps.Exceptions;
import janusgraph.util.batchimport.unsafe.helps.MovingAverage;
import janusgraph.util.batchimport.unsafe.helps.collection.concurrent.WorkSync;
import janusgraph.util.batchimport.unsafe.stats.ProcessingStats;
import janusgraph.util.batchimport.unsafe.stats.StatsProvider;
import janusgraph.util.batchimport.unsafe.stats.StepStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;

/**
 * Basic implementation of a {@link Step}. Does the most plumbing job of building a step implementation.
 */
public abstract class AbstractStep<T> implements Step<T>
{

    protected final StageControl control;
    private volatile String name;
    @SuppressWarnings( "rawtypes" )
    protected volatile Step downstream;
    protected volatile WorkSync<Downstream,SendDownstream> downstreamWorkSync;
    private volatile boolean endOfUpstream;
    protected volatile Throwable panic;
    private volatile boolean completed;
    protected int orderingGuarantees;

    // Milliseconds awaiting downstream to process batches so that its queue size goes beyond the configured threshold
    // If this is big then it means that this step is faster than downstream.
    protected final LongAdder downstreamIdleTime = new LongAdder();
    // Milliseconds awaiting upstream to hand over batches to this step.
    // If this is big then it means that this step is faster than upstream.
    protected final LongAdder upstreamIdleTime = new LongAdder();
    // Number of batches received, but not yet processed.
    protected final AtomicInteger queuedBatches = new AtomicInteger();
    // Number of batches fully processed
    protected final AtomicLong doneBatches = new AtomicLong();
    // Milliseconds spent processing all received batches.
    protected final MovingAverage totalProcessingTime;
    protected long startTime;
    protected long endTime;
    protected final List<StatsProvider> additionalStatsProvider;
    protected final Runnable healthChecker = this::assertHealthy;
    protected final Configuration config;

    public AbstractStep(StageControl control, String name, Configuration config,
                        StatsProvider... additionalStatsProvider )
    {
        this.control = control;
        this.name = name;
        this.config = config;
        this.totalProcessingTime = new MovingAverage( config.movingAverageSize() );
        this.additionalStatsProvider = asList( additionalStatsProvider );
    }

    @Override
    public void start( int orderingGuarantees )
    {
        this.orderingGuarantees = orderingGuarantees;
        resetStats();
    }

    protected boolean guarantees( int orderingGuaranteeFlag )
    {
        return (orderingGuarantees & orderingGuaranteeFlag) != 0;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public void receivePanic( Throwable cause )
    {
        this.panic = cause;
        this.completed = true;
    }

    protected boolean stillWorking()
    {
        if ( isPanic() )
        {   // There has been a panic, so we'll just stop working
            return false;
        }

        if ( endOfUpstream && queuedBatches.get() == 0 )
        {   // Upstream has run out and we've processed everything upstream sent us
            return false;
        }

        // We're still working
        return true;
    }

    protected boolean isPanic()
    {
        return panic != null;
    }

    @Override
    public boolean isCompleted()
    {
        return completed;
    }

    protected void issuePanic( Throwable cause )
    {
        issuePanic( cause, true );
    }

    protected void issuePanic( Throwable cause, boolean rethrow )
    {
        control.panic( cause );
        if ( rethrow )
        {
            throw Exceptions.launderedException( cause );
        }
    }

    protected void assertHealthy()
    {
        if ( isPanic() )
        {
            throw Exceptions.launderedException( panic );
        }
    }

    @Override
    public void setDownstream( Step<?> downstream )
    {
        assert downstream != this;
        this.downstream = downstream;
        //noinspection unchecked
        this.downstreamWorkSync = new WorkSync<>( new Downstream( (Step<Object>) downstream, doneBatches ) );
    }

    @Override
    public StepStats stats()
    {
        Collection<StatsProvider> providers = new ArrayList<>();
        collectStatsProviders( providers );
        return new StepStats( name, stillWorking(), providers );
    }

    protected void collectStatsProviders( Collection<StatsProvider> into )
    {
        into.add( new ProcessingStats( doneBatches.get() + queuedBatches.get(), doneBatches.get(),
                totalProcessingTime.total(), totalProcessingTime.average() / processors( 0 ),
                upstreamIdleTime.sum(), downstreamIdleTime.sum() ) );
        into.addAll( additionalStatsProvider );
    }

    @Override
    public void endOfUpstream()
    {
        endOfUpstream = true;
        checkNotifyEndDownstream();
    }

    protected void checkNotifyEndDownstream()
    {
        if ( !stillWorking() && !isCompleted() )
        {
            synchronized ( this )
            {
                // Only allow a single thread to notify that we've ended our stream as well as calling done()
                // stillWorking(), once false cannot again return true so no need to check
                if ( !isCompleted() )
                {
                    done();
                    if ( downstream != null )
                    {
                        downstream.endOfUpstream();
                    }
                    endTime = currentTimeMillis();
                    completed = true;
                }
            }
        }
    }

    /**
     * Called once, when upstream has run out of batches to send and all received batches have been
     * processed successfully.
     */
    protected void done()
    {
    }

    @Override
    public void close() throws Exception
    {   // Do nothing by default
    }

    protected void changeName( String name )
    {
        this.name = name;
    }

    protected void resetStats()
    {
        downstreamIdleTime.reset();
        upstreamIdleTime.reset();
        queuedBatches.set( 0 );
        doneBatches.set( 0 );
        totalProcessingTime.reset();
        startTime = currentTimeMillis();
        endTime = 0;
    }

    @Override
    public String toString()
    {
        return format( "%s[%s, processors:%d, batches:%d", getClass().getSimpleName(),
                name, processors( 0 ), doneBatches.get() );
    }
}
