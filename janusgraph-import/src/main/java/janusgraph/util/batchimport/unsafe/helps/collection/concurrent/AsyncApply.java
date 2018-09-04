package janusgraph.util.batchimport.unsafe.helps.collection.concurrent;

import java.util.concurrent.ExecutionException;

/**
 * The past or future application of work submitted asynchronously to a {@link WorkSync}.
 */
public interface AsyncApply
{
    /**
     * Await the application of the work submitted to a {@link WorkSync}.
     * <p>
     * If the work is already done, then this method with return immediately.
     * <p>
     * If the work has not been done, then this method will attempt to grab the {@code WorkSync} lock to complete the
     * work, or block to wait for another thread to complete the work on behalf of the current thread.
     *
     * @throws ExecutionException if this thread ends up performing the work, and an exception is thrown from the
     * attempt to apply the work.
     */
    void await() throws ExecutionException;
}
