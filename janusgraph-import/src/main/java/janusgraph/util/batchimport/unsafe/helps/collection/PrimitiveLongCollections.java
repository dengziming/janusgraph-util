package janusgraph.util.batchimport.unsafe.helps.collection;


import janusgraph.util.batchimport.unsafe.helps.Resource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;

import static janusgraph.util.batchimport.unsafe.helps.collection.PrimitiveCommons.closeSafely;

/**
 * Basic and common primitive int collection utils and manipulations.
 *
 */
public class PrimitiveLongCollections
{

    private static final PrimitiveLongIterator EMPTY = new PrimitiveLongBaseIterator()
    {
        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    };

    private PrimitiveLongCollections()
    {
    }

    public static PrimitiveLongIterator iterator( final long... items )
    {
        return new PrimitiveLongBaseIterator()
        {
            private int index = -1;

            @Override
            protected boolean fetchNext()
            {
                return ++index < items.length && next( items[index] );
            }
        };
    }

    public static PrimitiveLongIterator append( final PrimitiveLongIterator iterator, final long item )
    {
        return new PrimitiveLongBaseIterator()
        {
            private boolean singleItemReturned;

            @Override
            protected boolean fetchNext()
            {
                if ( iterator.hasNext() )
                {
                    return next( iterator.next() );
                }
                else if ( !singleItemReturned )
                {
                    singleItemReturned = true;
                    return next( item );
                }
                return false;
            }
        };
    }

    public static PrimitiveLongIterator filter( PrimitiveLongIterator source, final LongPredicate filter )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            @Override
            public boolean test( long item )
            {
                return filter.test( item );
            }
        };
    }

    public static PrimitiveLongResourceIterator filter( PrimitiveLongResourceIterator source, final LongPredicate filter )
    {
        return new PrimitiveLongResourceFilteringIterator( source )
        {
            @Override
            public boolean test( long item )
            {
                return filter.test( item );
            }
        };
    }

    public static PrimitiveLongIterator not( PrimitiveLongIterator source, final long disallowedValue )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            @Override
            public boolean test( long testItem )
            {
                return testItem != disallowedValue;
            }
        };
    }

    // Limitinglic
    public static PrimitiveLongIterator limit( final PrimitiveLongIterator source, final int maxItems )
    {
        return new PrimitiveLongBaseIterator()
        {
            private int visited;

            @Override
            protected boolean fetchNext()
            {
                if ( visited++ < maxItems )
                {
                    if ( source.hasNext() )
                    {
                        return next( source.next() );
                    }
                }
                return false;
            }
        };
    }

    public static PrimitiveLongIterator singleton( final long item )
    {
        return new PrimitiveLongBaseIterator()
        {
            private boolean returned;

            @Override
            protected boolean fetchNext()
            {
                try
                {
                    return !returned && next( item );
                }
                finally
                {
                    returned = true;
                }
            }
        };
    }

    public static long first( PrimitiveLongIterator iterator )
    {
        assertMoreItems( iterator );
        return iterator.next();
    }

    private static void assertMoreItems( PrimitiveLongIterator iterator )
    {
        if ( !iterator.hasNext() )
        {
            throw new NoSuchElementException( "No element in " + iterator );
        }
    }

    public static long first( PrimitiveLongIterator iterator, long defaultItem )
    {
        return iterator.hasNext() ? iterator.next() : defaultItem;
    }

    public static long last( PrimitiveLongIterator iterator )
    {
        assertMoreItems( iterator );
        return last( iterator, 0 /*will never be used*/ );
    }

    public static long last( PrimitiveLongIterator iterator, long defaultItem )
    {
        long result = defaultItem;
        while ( iterator.hasNext() )
        {
            result = iterator.next();
        }
        return result;
    }

    public static long single( PrimitiveLongIterator iterator )
    {
        try
        {
            assertMoreItems( iterator );
            long item = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new NoSuchElementException( "More than one item in " + iterator + ", first:" + item +
                        ", second:" + iterator.next() );
            }
            closeSafely( iterator );
            return item;
        }
        catch ( NoSuchElementException exception )
        {
            closeSafely( iterator, exception );
            throw exception;
        }
    }

    public static long single( PrimitiveLongIterator iterator, long defaultItem )
    {
        try
        {
            if ( !iterator.hasNext() )
            {
                closeSafely( iterator );
                return defaultItem;
            }
            long item = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new NoSuchElementException( "More than one item in " + iterator + ", first:" + item +
                        ", second:" + iterator.next() );
            }
            closeSafely( iterator );
            return item;
        }
        catch ( NoSuchElementException exception )
        {
            closeSafely( iterator, exception );
            throw exception;
        }
    }

    /**
     * Returns the index of the given item in the iterator(zero-based). If no items in {@code iterator}
     * equals {@code item} {@code -1} is returned.
     *
     * @param item the item to look for.
     * @param iterator of items.
     * @return index of found item or -1 if not found.
     */
    public static int indexOf( PrimitiveLongIterator iterator, long item )
    {
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            if ( item == iterator.next() )
            {
                return i;
            }
        }
        return -1;
    }

    /**
     * Validates whether two {@link Iterator}s are equal or not, i.e. if they have contain same number of items
     * and each orderly item equals one another.
     *
     * @param first the {@link Iterator} containing the first items.
     * @param other the {@link Iterator} containing the other items.
     * @return whether the two iterators are equal or not.
     */
    public static boolean equals( PrimitiveLongIterator first, PrimitiveLongIterator other )
    {
        boolean firstHasNext;
        boolean otherHasNext;
        // single | so that both iterator's hasNext() gets evaluated.
        while ( (firstHasNext = first.hasNext()) | (otherHasNext = other.hasNext()) )
        {
            if ( firstHasNext != otherHasNext || first.next() != other.next() )
            {
                return false;
            }
        }
        return true;
    }


    public static int count( PrimitiveLongIterator iterator )
    {
        int count = 0;
        for ( ; iterator.hasNext(); iterator.next(), count++ )
        {   // Just loop through this
        }
        return count;
    }

    public static boolean contains( long[] values, long candidate )
    {
        for ( int i = 0; i < values.length; i++ )
        {
            if ( values[i] == candidate )
            {
                return true;
            }
        }
        return false;
    }

    public static PrimitiveLongIterator emptyIterator()
    {
        return EMPTY;
    }

    public static <T> Iterator<T> map( final LongFunction<T> mapFunction, final PrimitiveLongIterator source )
    {
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return source.hasNext();
            }

            @Override
            public T next()
            {
                return mapFunction.apply( source.next() );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link List}, boxing each long.
     *
     * @param iterator {@link PrimitiveLongIterator} to pull values from.
     * @return a {@link List} containing all items.
     */
    public static List<Long> asList( PrimitiveLongIterator iterator )
    {
        List<Long> out = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            out.add( iterator.next() );
        }
        return out;
    }

    @SuppressWarnings( "UnusedDeclaration" )
    public static Iterator<Long> toIterator( final PrimitiveLongIterator primIterator )
    {
        return new Iterator<Long>()
        {
            @Override
            public boolean hasNext()
            {
                return primIterator.hasNext();
            }

            @Override
            public Long next()
            {
                return primIterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException(  );
            }
        };
    }

    /**
     * Wraps a {@link PrimitiveLongIterator} in a {@link PrimitiveLongResourceIterator} which closes
     * the provided {@code resource} in {@link PrimitiveLongResourceIterator#close()}.
     *
     * @param iterator {@link PrimitiveLongIterator} to convert
     * @param resource {@link Resource} to close in {@link PrimitiveLongResourceIterator#close()}
     * @return Wrapped {@link PrimitiveLongIterator}.
     */
    public static PrimitiveLongResourceIterator resourceIterator( final PrimitiveLongIterator iterator,
            final Resource resource )
    {
        return new PrimitiveLongResourceIterator()
        {
            @Override
            public void close()
            {
                if ( resource != null )
                {
                    resource.close();
                }
            }

            @Override
            public long next()
            {
                return iterator.next();
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }
        };
    }

    /**
     * Base iterator for simpler implementations of {@link PrimitiveLongIterator}s.
     */
    public abstract static class PrimitiveLongBaseIterator implements PrimitiveLongIterator
    {
        private boolean hasNextDecided;
        private boolean hasNext;
        protected long next;

        @Override
        public boolean hasNext()
        {
            if ( !hasNextDecided )
            {
                hasNext = fetchNext();
                hasNextDecided = true;
            }
            return hasNext;
        }

        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException( "No more elements in " + this );
            }
            hasNextDecided = false;
            return next;
        }

        /**
         * Fetches the next item in this iterator. Returns whether or not a next item was found. If a next
         * item was found, that value must have been set inside the implementation of this method
         * using {@link #next(long)}.
         */
        protected abstract boolean fetchNext();

        /**
         * Called from inside an implementation of {@link #fetchNext()} if a next item was found.
         * This method returns {@code true} so that it can be used in short-hand conditionals
         * (TODO what are they called?), like:
         * <pre>
         * protected boolean fetchNext()
         * {
         *     return source.hasNext() ? next( source.next() ) : false;
         * }
         * </pre>
         * @param nextItem the next item found.
         */
        protected boolean next( long nextItem )
        {
            next = nextItem;
            hasNext = true;
            return true;
        }
    }

    public static class PrimitiveLongConcatingIterator extends PrimitiveLongBaseIterator
    {
        private final Iterator<? extends PrimitiveLongIterator> iterators;
        private PrimitiveLongIterator currentIterator;

        public PrimitiveLongConcatingIterator( Iterator<? extends PrimitiveLongIterator> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext()
        {
            if ( currentIterator == null || !currentIterator.hasNext() )
            {
                while ( iterators.hasNext() )
                {
                    currentIterator = iterators.next();
                    if ( currentIterator.hasNext() )
                    {
                        break;
                    }
                }
            }
            return (currentIterator != null && currentIterator.hasNext()) && next( currentIterator.next() );
        }

    }

    public abstract static class PrimitiveLongFilteringIterator extends PrimitiveLongBaseIterator
            implements LongPredicate
    {
        protected final PrimitiveLongIterator source;

        PrimitiveLongFilteringIterator( PrimitiveLongIterator source )
        {
            this.source = source;
        }

        @Override
        protected boolean fetchNext()
        {
            while ( source.hasNext() )
            {
                long testItem = source.next();
                if ( test( testItem ) )
                {
                    return next( testItem );
                }
            }
            return false;
        }

        @Override
        public abstract boolean test( long testItem );
    }

    public abstract static class PrimitiveLongResourceFilteringIterator extends PrimitiveLongFilteringIterator
            implements PrimitiveLongResourceIterator
    {
        PrimitiveLongResourceFilteringIterator( PrimitiveLongIterator source )
        {
            super( source );
        }

        @Override
        public void close()
        {
            if ( source instanceof Resource )
            {
                ((Resource) source).close();
            }
        }
    }

    public static class PrimitiveLongRangeIterator extends PrimitiveLongBaseIterator
    {
        private long current;
        private final long end;
        private final long stride;

        PrimitiveLongRangeIterator( long start, long end, long stride )
        {
            this.current = start;
            this.end = end;
            this.stride = stride;
        }

        @Override
        protected boolean fetchNext()
        {
            try
            {
                return current <= end && next( current );
            }
            finally
            {
                current += stride;
            }
        }
    }
}
