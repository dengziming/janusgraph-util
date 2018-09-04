package janusgraph.util.batchimport.unsafe.helps.collection.concurrent;

/**
 * An end-point of sorts, through which events can be sent or queued up for background processing.
 *
 * @param <T> The type of {@code AsyncEvent} objects this {@code AsyncEventSender} and process.
 */
public interface AsyncEventSender<T extends AsyncEvent>
{
    /**
     * Send the given event to a background thread for processing.
     *
     * @param event The event that needs to be processed in the background.
     */
    void send(T event);
}
