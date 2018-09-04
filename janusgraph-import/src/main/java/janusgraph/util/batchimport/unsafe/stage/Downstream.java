package janusgraph.util.batchimport.unsafe.stage;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

class Downstream
{
    private static final java.util.Comparator<TicketedBatch> TICKETED_BATCH_COMPARATOR =
            ( a, b ) -> Long.compare( b.ticket, a.ticket );

    private final Step<Object> downstream;
    private final AtomicLong doneBatches;
    private final ArrayList<TicketedBatch> batches;
    private long lastSendTicket = -1;

    Downstream(Step<Object> downstream, AtomicLong doneBatches )
    {
        this.downstream = downstream;
        this.doneBatches = doneBatches;
        batches = new ArrayList<>();
    }

    long send()
    {
        // Sort in reverse, so the elements we want to send first are at the end.
        batches.sort( TICKETED_BATCH_COMPARATOR );
        long idleTimeSum = 0;
        long batchesDone = 0;

        for ( int i = batches.size() - 1; i >= 0 ; i-- )
        {
            TicketedBatch batch = batches.get( i );
            if ( batch.ticket == lastSendTicket + 1 )
            {
                batches.remove( i );
                lastSendTicket = batch.ticket;
                idleTimeSum += downstream.receive( batch.ticket, batch.batch );
                batchesDone++;
            }
            else
            {
                break;
            }
        }

        doneBatches.getAndAdd( batchesDone );
        return idleTimeSum;
    }

    void queue( TicketedBatch batch )
    {
        // Check that this is not a marker to flush the downstream.
        if ( batch.ticket != -1 && batch.batch != null )
        {
            batches.add( batch );
        }
    }
}
