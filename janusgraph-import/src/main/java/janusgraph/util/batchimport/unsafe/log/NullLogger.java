package janusgraph.util.batchimport.unsafe.log;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

/**
 * A {@link Logger} implementation that discards all messages
 */
public final class NullLogger implements Logger
{
    private static final NullLogger INSTANCE = new NullLogger();

    private NullLogger()
    {
    }

    /**
     * @return A singleton {@link NullLogger} instance
     */
    public static NullLogger getInstance()
    {
        return INSTANCE;
    }

    @Override
    public void log( @Nonnull String message )
    {
    }

    @Override
    public void log(@Nonnull String message, @Nonnull Throwable throwable )
    {
    }

    @Override
    public void log(@Nonnull String format, @Nonnull Object... arguments )
    {
    }

    @Override
    public void bulk( @Nonnull Consumer<Logger> consumer )
    {
    }
}
