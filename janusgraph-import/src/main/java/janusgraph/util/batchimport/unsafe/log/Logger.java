package janusgraph.util.batchimport.unsafe.log;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Consumer;

/**
 * A log into which messages can be written
 */
public interface Logger
{
    /**
     * @param message The message to be written
     */
    void log(@Nonnull String message);

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void log(@Nonnull String message, @Nonnull Throwable throwable);

    /**
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the {@code format}
     */
    void log(@Nonnull String format, @Nullable Object... arguments);

    /**
     * Used to temporarily write several messages in bulk. The implementation may choose to
     * disable flushing, and may also block other operations until the bulk update is completed.
     *
     * @param consumer A callback operation that accepts an equivalent {@link Logger}
     */
    void bulk(@Nonnull Consumer<Logger> consumer);
}
