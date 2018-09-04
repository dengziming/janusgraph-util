package janusgraph.util.batchimport.unsafe.helps.collection.concurrent;

/**
 * The base-class for events that can be processed with an {@link AsyncEvents} processor.
 */
public abstract class AsyncEvent
{
    volatile AsyncEvent next;
}
