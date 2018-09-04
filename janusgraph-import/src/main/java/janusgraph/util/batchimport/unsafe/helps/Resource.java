package janusgraph.util.batchimport.unsafe.helps;

/**
 * Resource that should be closed when not needed anymore. Extends {@link AutoCloseable}
 * with {@link #close()} not throwing any checked exception.
 */
public interface Resource extends AutoCloseable
{
    @Override
    void close();

    /**
     * Empty resource that doesn't {@link #close() close} anything.
     */
    Resource EMPTY = () ->
    {
        // Nothing to close
    };
}
