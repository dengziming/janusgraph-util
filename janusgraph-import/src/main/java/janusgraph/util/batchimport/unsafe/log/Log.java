package janusgraph.util.batchimport.unsafe.log;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * A log into which various levels of messages can be written
 */
public interface Log
{
    /**
     * @return true if the current log level enables debug logging
     */
    boolean isDebugEnabled();

    /**
     * @return a {@link Logger} instance for writing debug messages
     */
    @Nonnull
    Logger debugLogger();

    /**
     * Shorthand for {@code debugLogger().log( message )}
     *
     * @param message The message to be written
     */
    void debug(@Nonnull String message);

    /**
     * Shorthand for {@code debugLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void debug(@Nonnull String message, @Nonnull Throwable throwable);

    /**
     * Shorthand for {@code debugLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void debug(@Nonnull String format, @Nullable Object... arguments);

    /**
     * @return a {@link Logger} instance for writing info messages
     */
    @Nonnull
    Logger infoLogger();

    /**
     * Shorthand for {@code infoLogger().log( message )}
     *
     * @param message The message to be written
     */
    void info(@Nonnull String message);

    /**
     * Shorthand for {@code infoLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void info(@Nonnull String message, @Nonnull Throwable throwable);

    /**
     * Shorthand for {@code infoLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void info(@Nonnull String format, @Nullable Object... arguments);

    /**
     * @return a {@link Logger} instance for writing warn messages
     */
    @Nonnull
    Logger warnLogger();

    /**
     * Shorthand for {@code warnLogger().log( message )}
     *
     * @param message The message to be written
     */
    void warn(@Nonnull String message);

    /**
     * Shorthand for {@code warnLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void warn(@Nonnull String message, @Nonnull Throwable throwable);

    /**
     * Shorthand for {@code warnLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void warn(@Nonnull String format, @Nullable Object... arguments);

    /**
     * @return a {@link Logger} instance for writing error messages
     */
    @Nonnull
    Logger errorLogger();

    /**
     * Shorthand for {@code errorLogger().log( message )}
     *
     * @param message The message to be written
     */
    void error(@Nonnull String message);

    /**
     * Shorthand for {@code errorLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void error(@Nonnull String message, @Nonnull Throwable throwable);

    /**
     * Shorthand for {@code errorLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the {@code format}
     */
    void error(@Nonnull String format, @Nullable Object... arguments);

    /**
     * Used to temporarily log several messages in bulk. The implementation may choose to
     * disable flushing, and may also block other operations until the bulk update is completed.
     *
     * @param consumer A consumer for the bulk {@link Log}
     */
    void bulk(@Nonnull Consumer<Log> consumer);
}
