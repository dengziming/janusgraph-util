package janusgraph.util.batchimport.unsafe.helps;

import java.util.stream.Stream;

/**
 * {@link Iterable} whose {@link ResourceIterator iterators} have associated resources
 * that need to be released.
 *
 * {@link ResourceIterator ResourceIterators} are always automatically released when their owning
 * transaction is committed or rolled back.
 *
 * Inside a long running transaction, it is possible to release associated resources early. To do so
 * you must ensure that all returned ResourceIterators are either fully exhausted, or explicitly closed.
 * <p>
 * If you intend to exhaust the returned iterators, you can use conventional code as you would with a normal Iterable:
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * for ( Object item : iterable )
 * {
 *     ...
 * }
 * }
 * </pre>
 *
 * However, if your code might not exhaust the iterator, (run until {@link java.util.Iterator#hasNext()}
 * returns {@code false}), {@link ResourceIterator} provides you with a {@link ResourceIterator#close()} method that
 * can be invoked to release its associated resources early, by using a {@code finally}-block, or try-with-resource.
 *
 * <pre>
 * {@code
 * ResourceIterable<Object> iterable;
 * ResourceIterator<Object> iterator = iterable.iterator();
 * try
 * {
 *     while ( iterator.hasNext() )
 *     {
 *         Object item = iterator.next();
 *         if ( ... )
 *         {
 *             return item; // iterator may not be exhausted.
 *         }
 *     }
 * }
 * finally
 * {
 *     iterator.close();
 * }
 * }
 * </pre>
 *
 * @param <T> the type of values returned through the iterators
 *
 * @see ResourceIterator
 */
public interface ResourceIterable<T> extends Iterable<T>
{
    /**
     * Returns an {@link ResourceIterator iterator} with associated resources that may be managed.
     */
    @Override
    ResourceIterator<T> iterator();

    /**
     * @return this iterable as a {@link Stream}
     */
    default Stream<T> stream()
    {
        return iterator().stream();
    }
}
