package janusgraph.util.batchimport.unsafe.helps;

import java.util.Iterator;

/**
 * Wraps an {@link Iterator} so that it returns items of another type. The
 * iteration is done lazily.
 *
 * @param <T> the type of items to return
 * @param <U> the type of items to wrap/convert from
 */
public abstract class IteratorWrapper<T, U> implements Iterator<T>
{
    private Iterator<U> source;

    public IteratorWrapper(Iterator<U> iteratorToWrap )
    {
        this.source = iteratorToWrap;
    }

    @Override
    public boolean hasNext()
    {
        return this.source.hasNext();
    }

    @Override
    public T next()
    {
        return underlyingObjectToObject( this.source.next() );
    }

    @Override
    public void remove()
    {
        this.source.remove();
    }

    protected abstract T underlyingObjectToObject( U object );
}
