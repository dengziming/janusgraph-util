package janusgraph.util.batchimport.unsafe.log;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An abstract implementation of {@link Log}, providing implementations
 * for the shortcut methods (debug, info, warn, error) that delegate
 * to the appropriate {@link Logger} (as obtained by {@link Log#debugLogger()},
 * {@link Log#infoLogger()}, {@link Log#warnLogger()} and
 * {@link Log#errorLogger()} respectively).
 */
public abstract class AbstractLog implements Log
{
    @Override
    public void debug( @Nonnull String message )
    {
        debugLogger().log( message );
    }

    @Override
    public void debug(@Nonnull String message, @Nonnull Throwable throwable )
    {
        debugLogger().log( message, throwable );
    }

    @Override
    public void debug(@Nonnull String format, @Nullable Object... arguments )
    {
        debugLogger().log( format, arguments );
    }

    @Override
    public void info( @Nonnull String message )
    {
        infoLogger().log( message );
    }

    @Override
    public void info(@Nonnull String message, @Nonnull Throwable throwable )
    {
        infoLogger().log( message, throwable );
    }

    @Override
    public void info(@Nonnull String format, @Nullable Object... arguments )
    {
        infoLogger().log( format, arguments );
    }

    @Override
    public void warn( @Nonnull String message )
    {
        warnLogger().log( message );
    }

    @Override
    public void warn(@Nonnull String message, @Nonnull Throwable throwable )
    {
        warnLogger().log( message, throwable );
    }

    @Override
    public void warn(@Nonnull String format, @Nullable Object... arguments )
    {
        warnLogger().log( format, arguments );
    }

    @Override
    public void error( @Nonnull String message )
    {
        errorLogger().log( message );
    }

    @Override
    public void error(@Nonnull String message, @Nonnull Throwable throwable )
    {
        errorLogger().log( message, throwable );
    }

    @Override
    public void error(@Nonnull String format, @Nullable Object... arguments )
    {
        errorLogger().log( format, arguments );
    }
}
