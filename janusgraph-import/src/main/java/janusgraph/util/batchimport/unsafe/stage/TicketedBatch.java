package janusgraph.util.batchimport.unsafe.stage;

class TicketedBatch
{
    final long ticket;
    final Object batch;
    TicketedBatch next;

    TicketedBatch( long ticket, Object batch )
    {
        this.ticket = ticket;
        this.batch = batch;
    }
}
