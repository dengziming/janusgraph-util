package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.helps.collection.concurrent.Work;

import java.util.concurrent.atomic.LongAdder;

class SendDownstream implements Work<Downstream,SendDownstream>
{
    private final LongAdder downstreamIdleTime;
    private TicketedBatch head;
    private TicketedBatch tail;

    SendDownstream( long ticket, Object batch, LongAdder downstreamIdleTime )
    {
        this.downstreamIdleTime = downstreamIdleTime;
        TicketedBatch b = new TicketedBatch( ticket, batch );
        head = b;
        tail = b;
    }

    @Override
    public SendDownstream combine( SendDownstream work )
    {
        tail.next = work.head;
        tail = work.tail;
        return this;
    }

    @Override
    public void apply( Downstream downstream ) throws Exception
    {
        TicketedBatch next = head;
        do
        {
            downstream.queue( next );
            next = next.next;
        }
        while ( next != null );
        downstreamIdleTime.add( downstream.send() );
    }
}
