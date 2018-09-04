package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.stats.StatsProvider;

import static java.lang.System.nanoTime;

/**
 * {@link Step} that doesn't receive batches, doesn't send batches downstream; just processes data.
 */
public abstract class LonelyProcessingStep extends AbstractStep<Void>
{
    private final int batchSize;
    private int batch;
    private long lastProcessingTimestamp;

    public LonelyProcessingStep(StageControl control, String name, Configuration config,
                                StatsProvider... additionalStatsProviders )
    {
        super( control, name, config, additionalStatsProviders );
        this.batchSize = config.batchSize();
    }

    @Override
    public long receive( long ticket, Void nothing )
    {
        new Thread()
        {
            @Override
            public void run()
            {
                assertHealthy();
                try
                {
                    try
                    {
                        lastProcessingTimestamp = nanoTime();
                        process();
                    }
                    catch ( Throwable e )
                    {
                        // we need to update panic state before ending upstream and notifying executor that we completed
                        issuePanic( e );
                    }
                    finally
                    {
                        endOfUpstream();
                    }
                }
                catch ( Throwable e )
                {
                    // to avoid cases when we hide original panic problem
                    // check first if we already in panic state and if so - rethrow original panic cause
                    if ( !isPanic() )
                    {
                        issuePanic( e );
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }.start();
        return 0;
    }

    /**
     * Called once and signals the start of this step. Responsible for calling {@link #progress(long)}
     * at least now and then.
     */
    protected abstract void process() throws Throwable;

    /**
     * Called from {@link #process()}, reports progress so that statistics are updated appropriately.
     *
     * @param amount number of items processed since last call to this method.
     */
    protected void progress( long amount )
    {
        batch += amount;
        if ( batch >= batchSize )
        {
            int batches = batch / batchSize;
            batch %= batchSize;
            doneBatches.addAndGet( batches );
            long time = nanoTime();
            totalProcessingTime.add( time - lastProcessingTimestamp );
            lastProcessingTimestamp = time;
        }
    }
}
