package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;

import janusgraph.util.batchimport.unsafe.helps.Exceptions;
import janusgraph.util.batchimport.unsafe.helps.collection.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Utility for running a handful of {@link Runnable} in parallel, each in its own thread.
 * {@link Runnable} instances are {@link #start(Runnable) added and started} and the caller can
 * {@link #await()} them all to finish, returning a {@link Throwable error} if any thread encountered one so
 * that the caller can decide how to handle that error. Or caller can use {@link #awaitAndThrowOnError(Class)}
 * where error from any worker would be thrown from that method.
 *
 * It's basically like using an {@link ExecutorService}, but without that "baggage" and an easier usage
 * and less code in the scenario described above.
 *
 * @param <R> type of workers
 */
public class Workers<R extends Runnable> implements Iterable<R>
{
    private final List<Worker> workers = new ArrayList<>();
    private final String names;

    public Workers(String names )
    {
        this.names = names;
    }

    /**
     * Starts a thread to run {@code toRun}. Returns immediately.
     *
     * @param toRun worker to start and run among potentially other workers.
     */
    public void start( R toRun )
    {
        Worker worker = new Worker( names + "-" + workers.size(), toRun );
        worker.start();
        workers.add( worker );
    }

    public Throwable await() throws InterruptedException
    {
        Throwable error = null;
        for ( Worker worker : workers )
        {
            Throwable anError = worker.await();
            if ( error == null )
            {
                error = anError;
            }
        }
        return error;
    }

    public Throwable awaitStrict()
    {
        try
        {
            return await();
        }
        catch ( InterruptedException e )
        {
            throw handleInterrupted( e );
        }
    }

    public <EXCEPTION extends Throwable> void awaitAndThrowOnError( Class<EXCEPTION> launderingException )
            throws EXCEPTION, InterruptedException
    {
        Throwable error = await();
        if ( error != null )
        {
            throw Exceptions.launderedException( launderingException, error );
        }
    }

    public <EXCEPTION extends Throwable> void awaitAndThrowOnErrorStrict( Class<EXCEPTION> launderingException )
            throws EXCEPTION
    {
        try
        {
            awaitAndThrowOnError( launderingException );
        }
        catch ( InterruptedException e )
        {
            throw handleInterrupted( e );
        }
    }

    private RuntimeException handleInterrupted( InterruptedException e )
    {
        Thread.interrupted();
        return new RuntimeException( "Got interrupted while awaiting workers (" + names + ") to complete", e );
    }

    @Override
    public Iterator<R> iterator()
    {
        return Iterators.map(worker -> worker.toRun, workers.iterator() );
    }

    private class Worker extends Thread
    {
        private volatile Throwable error;
        private final R toRun;

        Worker( String name, R toRun )
        {
            super( name );
            this.toRun = toRun;
        }

        @Override
        public void run()
        {
            try
            {
                toRun.run();
            }
            catch ( Throwable t )
            {
                error = t;
                throw Exceptions.launderedException( t );
            }
        }

        protected synchronized Throwable await() throws InterruptedException
        {
            join();
            return error;
        }
    }
}
