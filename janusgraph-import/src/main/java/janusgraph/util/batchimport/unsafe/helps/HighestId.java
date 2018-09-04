package janusgraph.util.batchimport.unsafe.helps;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks a highest id when there are potentially multiple concurrent threads calling {@link #offer(long)}.
 */
public class HighestId
{
    private final AtomicLong highestId;

    public HighestId()
    {
        this( 0 );
    }

    public HighestId(long initialId )
    {
        this.highestId = new AtomicLong( initialId );
    }

    public void offer( long candidate )
    {
        long currentHighest;
        do
        {
            currentHighest = highestId.get();
            if ( candidate <= currentHighest )
            {
                return;
            }
        }
        while ( !highestId.compareAndSet( currentHighest, candidate ) );
    }

    public long get()
    {
        return highestId.get();
    }
}
