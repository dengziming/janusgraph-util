package janusgraph.util.batchimport.unsafe.stage;



import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.helps.Pair;
import janusgraph.util.batchimport.unsafe.helps.collection.PrefetchingIterator;
import janusgraph.util.batchimport.unsafe.stats.Key;
import janusgraph.util.batchimport.unsafe.stats.Stat;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import static janusgraph.util.batchimport.unsafe.helps.Exceptions.launderedException;
import static java.lang.System.currentTimeMillis;

/**
 * Default implementation of {@link StageControl}
 */
public class StageExecution implements StageControl, AutoCloseable
{
    private final String stageName;
    private final String part;
    private final Configuration config;
    private final Collection<Step<?>> pipeline;
    private long startTime;
    private final int orderingGuarantees;
    private volatile Throwable panic;
    private final boolean shouldRecycle;
    private final ConcurrentLinkedQueue<Object> recycled;

    public StageExecution(String stageName, String part, Configuration config, Collection<Step<?>> pipeline,
                          int orderingGuarantees )
    {
        this.stageName = stageName;
        this.part = part;
        this.config = config;
        this.pipeline = pipeline;
        this.orderingGuarantees = orderingGuarantees;
        this.shouldRecycle = (orderingGuarantees & Step.RECYCLE_BATCHES) != 0;
        this.recycled = shouldRecycle ? new ConcurrentLinkedQueue<>() : null;
    }

    public boolean stillExecuting()
    {
        for ( Step<?> step : pipeline )
        {
            if ( !step.isCompleted() )
            {
                return true;
            }
        }
        return false;
    }

    public void start()
    {
        this.startTime = currentTimeMillis();
        for ( Step<?> step : pipeline )
        {
            step.start( orderingGuarantees );
        }
    }

    public long getExecutionTime()
    {
        return currentTimeMillis() - startTime;
    }

    public String getStageName()
    {
        return stageName;
    }

    public String name()
    {
        return stageName + (part != null ? part : "");
    }

    public Configuration getConfig()
    {
        return config;
    }

    public Iterable<Step<?>> steps()
    {
        return pipeline;
    }

    /**
     * @param stat statistics {@link Key}.
     * @param trueForAscending {@code true} for ordering by ascending, otherwise descending.
     * @return the steps ordered by the {@link Stat#asLong() long value representation} of the given
     * {@code stat} accompanied a factor by how it compares to the next value, where a value close to
     * {@code 1.0} signals them being close to equal, and a value of for example {@code 0.5} signals that
     * the value of the current step is half that of the next step.
     */
    public Iterable<Pair<Step<?>,Float>> stepsOrderedBy(final Key stat, final boolean trueForAscending )
    {
        final List<Step<?>> steps = new ArrayList<>( pipeline );
        Collections.sort( steps, ( o1, o2 ) ->
        {
            Long stat1 = o1.stats().stat( stat ).asLong();
            Long stat2 = o2.stats().stat( stat ).asLong();
            return trueForAscending
                    ? stat1.compareTo( stat2 )
                    : stat2.compareTo( stat1 );
        } );

        return () -> new PrefetchingIterator<Pair<Step<?>,Float>>()
        {
            private final Iterator<Step<?>> source = steps.iterator();
            private Step<?> next = source.hasNext() ? source.next() : null;

            @Override
            protected Pair<Step<?>,Float> fetchNextOrNull()
            {
                if ( next == null )
                {
                    return null;
                }

                Step<?> current = next;
                next = source.hasNext() ? source.next() : null;
                float factor = next != null
                        ? (float) stat( current, stat ) / (float) stat( next, stat )
                        : 1.0f;
                return Pair.of( current, factor );
            }

            private long stat( Step<?> step, Key stat12 )
            {
                return step.stats().stat( stat12 ).asLong();
            }
        };
    }

    public int size()
    {
        return pipeline.size();
    }

    @Override
    public synchronized void panic( Throwable cause )
    {
        if ( panic == null )
        {
            panic = cause;
            for ( Step<?> step : pipeline )
            {
                step.receivePanic( cause );
            }
        }
        else
        {
            if ( !panic.equals( cause ) )
            {
                panic.addSuppressed( cause );
            }
        }
    }

    @Override
    public void assertHealthy()
    {
        if ( panic != null )
        {
            throw launderedException( panic );
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + name() + "]";
    }

    @Override
    public void recycle( Object batch )
    {
        if ( shouldRecycle )
        {
            recycled.offer( batch );
        }
    }

    @Override
    public <T> T reuse( Supplier<T> fallback )
    {
        if ( shouldRecycle )
        {
            @SuppressWarnings( "unchecked" )
            T result = (T) recycled.poll();
            if ( result != null )
            {
                return result;
            }
        }

        return fallback.get();
    }

    @Override
    public void close()
    {
        if ( shouldRecycle )
        {
            recycled.clear();
        }
    }
}
