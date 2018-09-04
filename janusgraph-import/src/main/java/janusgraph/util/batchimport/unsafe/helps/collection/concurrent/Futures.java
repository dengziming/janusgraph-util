package janusgraph.util.batchimport.unsafe.helps.collection.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Constructors for basic {@link Future} types
 */
public class Futures
{
    private Futures()
    {
    }

    /**
     * Combine multiple @{link Future} instances into a single Future
     *
     * @param futures the @{link Future} instances to combine
     * @param <V>     The result type returned by this Future's get method
     * @return A new @{link Future} representing the combination
     */
    @SafeVarargs
    public static <V> Future<List<V>> combine( final Future<? extends V>... futures )
    {
        return combine( Arrays.asList( futures ) );
    }

    /**
     * Combine multiple @{link Future} instances into a single Future
     *
     * @param futures the @{link Future} instances to combine
     * @param <V>     The result type returned by this Future's get method
     * @return A new @{link Future} representing the combination
     */
    public static <V> Future<List<V>> combine( final Iterable<? extends Future<? extends V>> futures )
    {
        return new Future<List<V>>()
        {
            @Override
            public boolean cancel( boolean mayInterruptIfRunning )
            {
                boolean result = false;
                for ( Future<? extends V> future : futures )
                {
                    result |= future.cancel( mayInterruptIfRunning );
                }
                return result;
            }

            @Override
            public boolean isCancelled()
            {
                boolean result = false;
                for ( Future<? extends V> future : futures )
                {
                    result |= future.isCancelled();
                }
                return result;
            }

            @Override
            public boolean isDone()
            {
                boolean result = false;
                for ( Future<? extends V> future : futures )
                {
                    result |= future.isDone();
                }
                return result;
            }

            @Override
            public List<V> get() throws InterruptedException, ExecutionException
            {
                List<V> result = new ArrayList<>();
                for ( Future<? extends V> future : futures )
                {
                    result.add( future.get() );
                }
                return result;
            }

            @Override
            public List<V> get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
            {
                List<V> result = new ArrayList<>();
                for ( Future<? extends V> future : futures )
                {
                    long before = System.nanoTime();
                    result.add( future.get( timeout, unit ) );
                    timeout -= unit.convert( System.nanoTime() - before, TimeUnit.NANOSECONDS );
                }
                return result;
            }
        };
    }
}
