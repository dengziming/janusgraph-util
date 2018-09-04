package janusgraph.util.batchimport.unsafe.helps.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Just like {@link Iterator}, but with the addition that {@link #hasNext()} and {@link #next()} can
 * be declared to throw a checked exception.
 *
 * @param <T> type of items in this iterator.
 * @param <EXCEPTION> type of exception thrown from {@link #hasNext()} and {@link #next()}.
 */
public interface RawIterator<T,EXCEPTION extends Exception>
{
    boolean hasNext() throws EXCEPTION;

    T next() throws EXCEPTION;

    default void remove()
    {
        throw new UnsupportedOperationException();
    }

    RawIterator<Object,Exception> EMPTY = RawIterator.of();

    @SuppressWarnings( "unchecked" )
    static <T, EXCEPTION extends Exception> RawIterator<T,EXCEPTION> empty()
    {
        return (RawIterator<T,EXCEPTION>) EMPTY;
    }

    static <T, EX extends Exception> RawIterator<T, EX> of(T... values)
    {
        return new RawIterator<T,EX>()
        {
            private int position;

            @Override
            public boolean hasNext() throws EX
            {
                return position < values.length;
            }

            @Override
            public T next() throws EX
            {
                if ( hasNext() )
                {
                    return values[position++];
                }
                throw new NoSuchElementException();
            }
        };
    }


    /**
     * Create a raw iterator from a regular iterator, assuming no exceptions are being thrown
     */
    static <T, EX extends Exception> RawIterator<T, EX> wrap(final Iterator<T> iterator)
    {
        return new RawIterator<T,EX>()
        {
            @Override
            public boolean hasNext() throws EX
            {
                return iterator.hasNext();
            }

            @Override
            public T next() throws EX
            {
                return iterator.next();
            }
        };
    }
}
