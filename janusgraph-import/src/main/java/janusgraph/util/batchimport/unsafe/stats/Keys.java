package janusgraph.util.batchimport.unsafe.stats;

/**
 * Common {@link Stat statistic} keys.
 */
public enum Keys implements Key
{
    received_batches( ">", "Number of batches received from upstream" ),
    done_batches( "!", "Number of batches processed and done, and sent off downstream" ),
    total_processing_time( "=", "Total processing time for all done batches" ),
    upstream_idle_time( "^", "Time spent waiting for batch from upstream" ),
    downstream_idle_time( "v", "Time spent waiting for downstream to catch up" ),
    avg_processing_time( "avg", "Average processing time per done batch" ),
    io_throughput( null, "I/O throughput per second" ),
    memory_usage( null, "Memory usage" ),
    progress( null, "Progress" ); // overrides progress calculation using done_batches, if this stat exists

    private final String shortName;
    private final String description;

    Keys( String shortName, String description )
    {
        this.shortName = shortName;
        this.description = description;
    }

    @Override
    public String shortName()
    {
        return shortName;
    }

    @Override
    public String description()
    {
        return description;
    }
}
