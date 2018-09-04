package janusgraph.util.batchimport.unsafe.helps;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;

/**
 * Closeable Iterator with associated resources.
 *
 * The associated resources are always released when the owning transaction is committed or rolled back.
 * The resource may also be released eagerly by explicitly calling {@link ResourceIterator#close()}
 * or by exhausting the iterator.
 *
 * @param <T> type of values returned by this Iterator
 *
 * @see ResourceIterable
 */
public interface ResourceIterator<T> extends Iterator<T>, Resource
{
    /**
     * Close the iterator early, freeing associated resources
     *
     * It is an error to use the iterator after this has been called.
     */
    @Override
    void close();

    /**
     * @return this iterator as a {@link Stream}
     */
    default Stream<T> stream()
    {
        return StreamSupport
                .stream( spliteratorUnknownSize( this, 0 ), false )
                .onClose( this::close );
    }

    default <R> ResourceIterator<R> map(Function<T, R> map)
    {
        return new ResourceIterator<R>()
        {
            @Override
            public void close()
            {
                ResourceIterator.this.close();
            }

            @Override
            public boolean hasNext()
            {
                return ResourceIterator.this.hasNext();
            }

            @Override
            public R next()
            {
                return map.apply( ResourceIterator.this.next() );
            }
        };
    }

    @SuppressWarnings( "unchecked" )
    static <T> ResourceIterator<T> empty()
    {
        return new ResourceIterator()
        {
            @Override
            public boolean hasNext()
            {
                return false;
            }

            @Override
            public T next()
            {
                return (T)Collections.EMPTY_LIST.iterator().next();
            }

            @Override
            public void close()
            {
            }
        };
    }
}
