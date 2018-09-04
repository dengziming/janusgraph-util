package janusgraph.util.batchimport.unsafe.helps.collection;

import janusgraph.util.batchimport.unsafe.helps.Predicates;
import janusgraph.util.batchimport.unsafe.helps.Resource;
import janusgraph.util.batchimport.unsafe.helps.ResourceIterator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class Iterables
{

    private Iterables()
    {
    }

    @SuppressWarnings( "unchecked" )
    public static <T> Iterable<T> empty()
    {
        return Collections.emptyList();
    }

    public static <T> Iterable<T> limit( final int limitItems, final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<T>()
            {
                int count;

                @Override
                public boolean hasNext()
                {
                    return count < limitItems && iterator.hasNext();
                }

                @Override
                public T next()
                {
                    count++;
                    return iterator.next();
                }

                @Override
                public void remove()
                {
                    iterator.remove();
                }
            };
        };
    }

    public static <T> Function<Iterable<T>, Iterable<T>> limit( final int limitItems )
    {
        return ts -> limit( limitItems, ts );
    }

    public static <T> Iterable<T> unique( final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<T>()
            {
                Set<T> items = new HashSet<>();
                T nextItem;

                @Override
                public boolean hasNext()
                {
                    while ( iterator.hasNext() )
                    {
                        nextItem = iterator.next();
                        if ( items.add( nextItem ) )
                        {
                            return true;
                        }
                    }

                    return false;
                }

                @Override
                public T next()
                {
                    if ( nextItem == null && !hasNext() )
                    {
                        throw new NoSuchElementException();
                    }

                    return nextItem;
                }

                @Override
                public void remove()
                {
                }
            };
        };
    }

    public static <T, C extends Collection<T>> C addAll( C collection, Iterable<? extends T> iterable )
    {
        Iterator<? extends T> iterator = iterable.iterator();
        try
        {
            while ( iterator.hasNext() )
            {
                collection.add( iterator.next() );
            }
        }
        finally
        {
            if ( iterator instanceof AutoCloseable )
            {
                try
                {
                    ((AutoCloseable) iterator).close();
                }
                catch ( Exception e )
                {
                    // Ignore
                }
            }
        }

        return collection;
    }


    public static <FROM, TO> Iterable<TO> map( Function<? super FROM, ? extends TO> function, Iterable<FROM> from )
    {
        return new MapIterable<>( from, function );
    }

    @SafeVarargs
    @SuppressWarnings( "unchecked" )
    public static <T, C extends T> Iterable<T> iterable( C... items )
    {
        return (Iterable<T>) Arrays.asList( items );
    }

    public static <T, C extends T> Iterable<T> append( final C item, final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<T>()
            {
                T last = item;

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext() || last != null;
                }

                @Override
                public T next()
                {
                    if ( iterator.hasNext() )
                    {
                        return iterator.next();
                    }
                    try
                    {
                        return last;
                    }
                    finally
                    {
                        last = null;
                    }
                }

                @Override
                public void remove()
                {
                }
            };
        };
    }

    public static <T> Iterable<T> cache( Iterable<T> iterable )
    {
        return new CacheIterable<>( iterable );
    }

    public static String toString( Iterable<?> values, String separator )
    {
        Iterator<?> it = values.iterator();
        StringBuilder sb = new StringBuilder();
        while ( it.hasNext() )
        {
            sb.append( it.next().toString() );
            if ( it.hasNext() )
            {
                sb.append( separator );
            }
        }
        return sb.toString();
    }

    /**
     * Returns the given iterable's first element or {@code null} if no
     * element found.
     *
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull( Iterable<T> iterable )
    {
        Iterator<T> iterator = iterable.iterator();
        try
        {
            return Iterators.firstOrNull( iterator );
        }
        finally
        {
            if ( iterator instanceof Resource )
            {
                ((Resource) iterator).close();
            }
        }
    }

    /**
     * Returns the given iterable's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first( Iterable<T> iterable )
    {
        return Iterators.first( iterable.iterator() );
    }

    /**
     * Returns the given iterable's last element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the last element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T last( Iterable<T> iterable )
    {
        return Iterators.last( iterable.iterator() );
    }



    /**
     * Returns the given iterable's single element or {@code itemIfNone} if no
     * element found. If there is more than one element in the iterable a
     * {@link NoSuchElementException} will be thrown.
     *
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @param itemIfNone item to use if none is found
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T single( Iterable<T> iterable, T itemIfNone )
    {
        return Iterators.single( iterable.iterator(), itemIfNone );
    }

    /**
     * Adds all the items in {@code iterator} to {@code collection}.
     * @param <C> the type of {@link Collection} to add to items to.
     * @param <T> the type of items in the collection and iterator.
     * @param iterable the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    private static <C extends Collection<T>,T> C addToCollection(Iterable<T> iterable,
                                                                 C collection)
    {
        return Iterators.addToCollection( iterable.iterator(), collection );
    }

    /**
     * Counts the number of items in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @return the number of items found in {@code iterable}.
     */
    public static <T> long count( Iterable<T> iterable )
    {
        return count( iterable, Predicates.alwaysTrue() );
    }

    /**
     * Counts the number of filtered items in the {@code iterable} by looping through it.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @param filter the filter to test items against
     * @return the number of found in {@code iterable}.
     */
    public static <T> long count( Iterable<T> iterable, Predicate<T> filter )
    {
        Iterator<T> iterator = iterable.iterator();
        try
        {
            return Iterators.count( iterator, filter );
        }
        finally
        {
            if ( iterator instanceof ResourceIterator)
            {
                ((ResourceIterator) iterator).close();
            }
        }
    }

    public static <T> List<T> asList( Iterable<T> iterator )
    {
        return addToCollection( iterator, new ArrayList<T>() );
    }

    private static class CacheIterable<T> implements Iterable<T>
    {
        private final Iterable<T> iterable;
        private Iterable<T> cache;

        private CacheIterable( Iterable<T> iterable )
        {
            this.iterable = iterable;
        }

        @Override
        public Iterator<T> iterator()
        {
            if ( cache != null )
            {
                return cache.iterator();
            }

            final Iterator<T> source = iterable.iterator();

            return new Iterator<T>()
            {
                List<T> iteratorCache = new ArrayList<>();

                @Override
                public boolean hasNext()
                {
                    boolean hasNext = source.hasNext();
                    if ( !hasNext )
                    {
                        cache = iteratorCache;
                    }
                    return hasNext;
                }

                @Override
                public T next()
                {
                    T next = source.next();
                    iteratorCache.add( next );
                    return next;
                }

                @Override
                public void remove()
                {

                }
            };
        }
    }

    /**
     * Returns the index of the first occurrence of the specified element
     * in this iterable, or -1 if this iterable does not contain the element.
     * More formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     *
     * @param itemToFind element to find
     * @param iterable iterable to look for the element in
     * @param <T> the type of the elements
     * @return the index of the first occurrence of the specified element
     *         (or {@code null} if that was specified) or {@code -1}
     */
    public static <T> int indexOf( T itemToFind, Iterable<T> iterable )
    {
        if ( itemToFind == null )
        {
            int index = 0;
            for ( T item : iterable )
            {
                if ( item == null )
                {
                    return index;
                }
                index++;
            }
        }
        else
        {
            int index = 0;
            for ( T item : iterable )
            {
                if ( itemToFind.equals( item ) )
                {
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    @SuppressWarnings( "rawtypes" )
    public static <T, S extends Comparable> Iterable<T> sort( Iterable<T> iterable, final Function<T, S> compareFunction )
    {
        List<T> list = asList( iterable );
        Collections.sort( list, Comparator.comparing( compareFunction::apply ) );
        return list;
    }

    /**
     * Create a stream from the given iterable.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream( Iterable<T> iterable )
    {
        return stream( iterable, 0 );
    }

    /**
     * Create a stream from the given iterable with given characteristics.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param characteristics the logical OR of characteristics for the underlying {@link Spliterator}
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream( Iterable<T> iterable, int characteristics )
    {
        Objects.requireNonNull( iterable );
        return Iterators.stream( iterable.iterator(), characteristics );
    }

}
