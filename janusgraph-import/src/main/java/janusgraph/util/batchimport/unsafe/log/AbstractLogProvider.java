package janusgraph.util.batchimport.unsafe.log;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * An abstract {@link LogProvider} implementation, which ensures {@link Log}s are cached and reused.
 */
public abstract class AbstractLogProvider<T extends Log> implements LogProvider
{
    private final ConcurrentHashMap<String, T> logCache = new ConcurrentHashMap<>();

    @Override
    public T getLog( final Class loggingClass )
    {
        return getLog( loggingClass.getName(), () -> buildLog( loggingClass ) );
    }

    @Override
    public T getLog( final String name )
    {
        return getLog( name, () -> buildLog( name ) );
    }

    private T getLog( String name, Supplier<T> logSupplier )
    {
        T log = logCache.get( name );
        if ( log == null )
        {
            T newLog = logSupplier.get();
            log = logCache.putIfAbsent( name, newLog );
            if ( log == null )
            {
                log = newLog;
            }
        }
        return log;
    }

    /**
     * @return a {@link Collection} of the {@link Log} mappings that are currently held in the cache
     */
    protected Collection<T> cachedLogs()
    {
        return logCache.values();
    }

    /**
     * @param loggingClass the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the {@code loggingClass} as the context
     */
    protected abstract T buildLog( Class loggingClass );

    /**
     * @param name the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the specified name as the context
     */
    protected abstract T buildLog( String name );
}
