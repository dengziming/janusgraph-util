package janusgraph.util.batchimport.unsafe.log;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * A {@link Log} implementation that discards all messages
 */
public final class NullLog implements Log
{
    private static final NullLog INSTANCE = new NullLog();

    private NullLog()
    {
    }

    /**
     * @return A singleton {@link NullLog} instance
     */
    public static NullLog getInstance()
    {
        return INSTANCE;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return false;
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void debug( @Nonnull String message )
    {
    }

    @Override
    public void debug(@Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void debug(@Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void info( @Nonnull String message )
    {
    }

    @Override
    public void info(@Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void info(@Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void warn( @Nonnull String message )
    {
    }

    @Override
    public void warn(@Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void warn(@Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public void error( @Nonnull String message )
    {
    }

    @Override
    public void error(@Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void error(@Nonnull String format, @Nullable Object... arguments )
    {
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        consumer.accept( this );
    }
}
