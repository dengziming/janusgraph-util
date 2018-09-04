package janusgraph.util.batchimport.unsafe.helps;

import java.util.concurrent.*;
import java.util.function.Supplier;

public abstract class FutureAdapter<V> implements Future<V>
{
    public static final Future<Void> VOID = CompletableFuture.completedFuture( null );

    @Override
    public boolean cancel( boolean mayInterruptIfRunning )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * This class will be deleted as part of next major release. Please use {@link CompletableFuture#complete(Object)}
     * instead.
     */
    @Deprecated
    public static class Present<V> extends FutureAdapter<V>
    {
        private final V value;

        public Present( V value )
        {
            this.value = value;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public V get()
        {
            return value;
        }
        @Override
        public V get( long timeout, TimeUnit unit )
        {
            return value;
        }
    }

    /**
     * @param <T> type of values that this {@link Future} have.
     * @param value result value.
     * @return {@link Present} future with already specified result
     *
     * This method will be deleted as part of next major release. Please use {@link CompletableFuture#complete(Object)}
     * instead.
     */
    @Deprecated
    public static <T> Present<T> present( T value )
    {
        return new Present<>( value );
    }

    public static <T> Future<T> latchGuardedValue( final Supplier<T> supplier, final CountDownLatch guardedByLatch,
                                                   final String jobDescription )
    {
        return new FutureAdapter<T>()
        {
            @Override
            public boolean isDone()
            {
                return guardedByLatch.getCount() == 0;
            }

            @Override
            public T get() throws InterruptedException, ExecutionException
            {
                guardedByLatch.await();
                return supplier.get();
            }

            @Override
            public T get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException,
                    TimeoutException
            {
                if ( !guardedByLatch.await( timeout, unit ) )
                {
                    throw new TimeoutException( jobDescription + " didn't complete within " +
                            timeout + " " + unit );
                }
                return supplier.get();
            }
        };
    }

    public static Future<Integer> processFuture( final Process process )
    {
        return new FutureAdapter<Integer>()
        {
            @Override
            public boolean isDone()
            {
                return tryGetExitValue( process ) != null;
            }

            private Integer tryGetExitValue( final Process process )
            {
                try
                {
                    return process.exitValue();
                }
                catch ( IllegalThreadStateException e )
                {   // Thrown if this process hasn't exited yet.
                    return null;
                }
            }

            @Override
            public Integer get() throws InterruptedException
            {
                return process.waitFor();
            }

            @Override
            public Integer get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException,
                    TimeoutException
            {
                long end = System.currentTimeMillis() + unit.toMillis( timeout );
                while ( System.currentTimeMillis() < end )
                {
                    Integer result = tryGetExitValue( process );
                    if ( result != null )
                    {
                        return result;
                    }
                    Thread.sleep( 10 );
                }
                throw new TimeoutException( "Process '" + process + "' didn't exit within " + timeout + " " + unit );
            }
        };
    }

    public static <T> Future<T> future( final Callable<T> task )
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit( task );
        executor.shutdown();
        return future;
    }
}
