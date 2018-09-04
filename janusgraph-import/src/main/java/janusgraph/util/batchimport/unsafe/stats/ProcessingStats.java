package janusgraph.util.batchimport.unsafe.stats;


import janusgraph.util.batchimport.unsafe.stage.Step;

import static janusgraph.util.batchimport.unsafe.stats.Stats.longStat;

/**
 * Provides common {@link Stat statistics} about a {@link Step}, stats like number of processed batches,
 * processing time a.s.o.
 */
public class ProcessingStats extends GenericStatsProvider
{
    public ProcessingStats(
            long receivedBatches, long doneBatches,
            long totalProcessingTime, long average,
            long upstreamIdleTime, long downstreamIdleTime )
    {
        add( Keys.received_batches, Stats.longStat( receivedBatches ) );
        add( Keys.done_batches, Stats.longStat( doneBatches ) );
        add( Keys.total_processing_time, Stats.longStat( totalProcessingTime ) );
        add( Keys.upstream_idle_time, Stats.longStat( upstreamIdleTime ) );
        add( Keys.downstream_idle_time, Stats.longStat( downstreamIdleTime ) );
        add( Keys.avg_processing_time, Stats.longStat( average ) );
    }
}
