package janusgraph.util.batchimport.unsafe.helps.collection;

import java.util.NoSuchElementException;

import static java.util.Arrays.copyOf;

/**
 * Like a {@code Stack<Long>} but for primitive longs. Virtually GC free in that it has an {@code long[]}
 * and merely moves a cursor where to {@link #push(long)} and {@link #poll()} values to and from.
 * If many items goes in the stack the {@code long[]} will grow to accomodate all of them, but not shrink again.
 */
public class PrimitiveLongStack implements PrimitiveLongCollection
{
    private long[] array;
    private int cursor = -1; // where the top most item lives

    public PrimitiveLongStack( )
    {
        this( 16 );
    }

    public PrimitiveLongStack(int initialSize )
    {
        this.array = new long[initialSize];
    }

    @Override
    public boolean isEmpty()
    {
        return cursor == -1;
    }

    @Override
    public void clear()
    {
        cursor = -1;
    }

    @Override
    public int size()
    {
        return cursor + 1;
    }

    @Override
    public void close()
    {   // Nothing to close
    }

    @Override
    public PrimitiveLongIterator iterator()
    {
        return new PrimitiveLongIterator()
        {
            int idx;

            @Override
            public boolean hasNext()
            {
                return idx <= cursor;
            }

            @Override
            public long next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }

                return array[idx++];
            }
        };
    }

    @Override
    public void visitKeys( PrimitiveLongVisitor visitor )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    public void push( long value )
    {
        ensureCapacity();
        array[++cursor] = value;
    }

    private void ensureCapacity()
    {
        if ( cursor == array.length - 1 )
        {
            array = copyOf( array, array.length << 1 );
        }
    }

    /**
     * @return the top most item, or -1 if stack is empty
     */
    public long poll()
    {
        return cursor == -1 ? -1 : array[cursor--];
    }
}
