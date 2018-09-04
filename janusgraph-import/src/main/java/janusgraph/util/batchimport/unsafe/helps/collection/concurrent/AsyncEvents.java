package janusgraph.util.batchimport.unsafe.helps.collection.concurrent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * {@code AsyncEvents} is a mechanism for queueing up events to be processed asynchronously in a background thread.
 *
 * The {@code AsyncEvents} object implements {@link Runnable}, so it can be passed to a thread pool, or given to a
 * dedicated thread. The runnable will then occupy a thread and dedicate it to background processing of events, until
 * the {@code AsyncEvents} is {@link AsyncEvents#shutdown()}.
 *
 * If events are sent to an {@code AsyncEvents} that has been shut down, then those events will be processed in the
 * foreground as a fall-back.
 *
 * Note, however, that no events are processed until the background thread is started.
 *
 * The {@code AsyncEvents} is given a {@link Consumer} of the specified event type upon construction, and will use it
 * for doing the actual processing of events once they have been collected.
 *
 * @param <T> The type of events the {@code AsyncEvents} will process.
 */
public class AsyncEvents<T extends AsyncEvent> implements AsyncEventSender<T>, Runnable
{
    public interface Monitor
    {
        void eventCount(long count);

        Monitor NONE = count -> {};
    }

    // TODO use VarHandles in Java 9
    private static final AtomicReferenceFieldUpdater<AsyncEvents,AsyncEvent> STACK =
            AtomicReferenceFieldUpdater.newUpdater( AsyncEvents.class, AsyncEvent.class, "stack" );
    private static final Sentinel END_SENTINEL = new Sentinel( "END" );
    private static final Sentinel SHUTDOWN_SENTINEL = new Sentinel( "SHUTDOWN" );

    private final Consumer<T> eventConsumer;
    private final Monitor monitor;
    private final BinaryLatch startupLatch;
    private final BinaryLatch shutdownLatch;

    @SuppressWarnings( {"unused", "FieldCanBeLocal"} )
    private volatile AsyncEvent stack; // Accessed via AtomicReferenceFieldUpdater
    private volatile Thread backgroundThread;
    private volatile boolean shutdown;

    /**
     * Construct a new {@code AsyncEvents} instance, that will use the given consumer to process the events.
     *
     * @param eventConsumer The {@link Consumer} used for processing the events that are sent in.
     */
    public AsyncEvents( Consumer<T> eventConsumer, Monitor monitor )
    {
        this.eventConsumer = eventConsumer;
        this.monitor = monitor;
        this.startupLatch = new BinaryLatch();
        this.shutdownLatch = new BinaryLatch();
        this.stack = END_SENTINEL;
    }

    @Override
    public void send( T event )
    {
        AsyncEvent prev = STACK.getAndSet( this, event );
        assert prev != null;
        event.next = prev;
        if ( prev == END_SENTINEL )
        {
            LockSupport.unpark( backgroundThread );
        }
        else if ( prev == SHUTDOWN_SENTINEL )
        {
            AsyncEvent events = STACK.getAndSet( this, SHUTDOWN_SENTINEL );
            process( events );
        }
    }

    @Override
    public void run()
    {
        assert backgroundThread == null : "A thread is already running " + backgroundThread;
        backgroundThread = Thread.currentThread();
        startupLatch.release();

        try
        {
            do
            {
                AsyncEvent events = STACK.getAndSet( this, END_SENTINEL );
                process( events );
                if ( stack == END_SENTINEL && !shutdown )
                {
                    LockSupport.park( this );
                }
            }
            while ( !shutdown );

            AsyncEvent events = STACK.getAndSet( this, SHUTDOWN_SENTINEL );
            process( events );
        }
        finally
        {
            backgroundThread = null;
            shutdownLatch.release();
        }
    }

    private void process( AsyncEvent events )
    {
        events = reverseAndStripEndMark( events );
        Consumer<T> consumer = this.eventConsumer;

        while ( events != null )
        {
            @SuppressWarnings( "unchecked" )
            T event = (T) events;
            consumer.accept( event );
            events = events.next;
        }
    }

    private AsyncEvent reverseAndStripEndMark( AsyncEvent events )
    {
        AsyncEvent result = null;
        long count = 0;
        while ( events != END_SENTINEL && events != SHUTDOWN_SENTINEL )
        {
            AsyncEvent next;
            do
            {
                next = events.next;
            }
            while ( next == null );
            events.next = result;
            result = events;
            events = next;
            count++;
        }
        if ( count > 0 )
        {
            monitor.eventCount( count );
        }
        return result;
    }

    /**
     * Initiate the shut down process of this {@code AsyncEvents} instance.
     *
     * This call does not block or otherwise wait for the background thread to terminate.
     */
    public void shutdown()
    {
        assert !shutdown : "Already shut down";
        shutdown = true;
        LockSupport.unpark( backgroundThread );
    }

    public void awaitStartup()
    {
        startupLatch.await();
    }

    public void awaitTermination() throws InterruptedException
    {
        shutdownLatch.await();
    }

    private static class Sentinel extends AsyncEvent
    {
        private final String str;

        Sentinel( String identifier )
        {
            this.str = "AsyncEvent/Sentinel[" + identifier + "]";
        }

        @Override
        public String toString()
        {
            return str;
        }
    }
}
