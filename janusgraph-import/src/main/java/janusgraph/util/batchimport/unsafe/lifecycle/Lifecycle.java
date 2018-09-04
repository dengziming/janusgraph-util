package janusgraph.util.batchimport.unsafe.lifecycle;

/**
 * Lifecycle interface for kernel components. Init is called first,
 * followed by start,
 * and then any number of stop-start sequences,
 * and finally stop and shutdown.
 *
 * As a stop-start cycle could be due to change of configuration, please perform anything that depends on config
 * in start().
 *
 * Implementations can throw any exception. Caller must handle this properly.
 *
 * The primary purpose of init in a component is to set up structure: instantiate dependent objects,
 * register handlers/listeners, etc.
 * Only in start should the component actually do anything with this structure.
 * Stop reverses whatever was done in start, and shutdown finally clears any set-up structure, if necessary.
 */
public interface Lifecycle
{
    void init() throws Throwable;

    void start() throws Throwable;

    void stop() throws Throwable;

    void shutdown() throws Throwable;

}
