package janusgraph.util.batchimport.unsafe.helps.collection;

import java.util.Iterator;

class WrappingResourceIterator<T> extends PrefetchingResourceIterator<T>
{
    private final Iterator<T> iterator;

    WrappingResourceIterator( Iterator<T> iterator )
    {
        this.iterator = iterator;
    }

    @Override
    public void close()
    {
    }

    @Override
    public void remove()
    {
        iterator.remove();
    }

    @Override
    protected T fetchNextOrNull()
    {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
