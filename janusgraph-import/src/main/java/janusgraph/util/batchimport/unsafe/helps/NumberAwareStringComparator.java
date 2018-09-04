package janusgraph.util.batchimport.unsafe.helps;

import janusgraph.util.batchimport.unsafe.helps.collection.PrefetchingIterator;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Comparator for strings that may, or may not, contain groups of digits representing numbers and where
 * those numbers should be compared for what their numeric values are, not their string representations.
 * This will solve a classic sorting issue that plain string sorting misses:
 * <ol>
 * <li>string-1</li>
 * <li>string-2</li>
 * <li>string-12</li>
 * </ol>
 * Where the above would be sorted as {@code string-1}, {@code string-12}, {@code string-2}, which may be
 * undesirable in scenarios where the number matters. This comparator will sort the strings from the
 * example above as {@code string-1}, {@code string-2}, {@code string-12}.
 */
public class NumberAwareStringComparator implements Comparator<String>
{
    public static final Comparator<String> INSTANCE = new NumberAwareStringComparator();

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    @Override
    public int compare( String o1, String o2 )
    {
        Iterator<Comparable> c1 = comparables( o1 );
        Iterator<Comparable> c2 = comparables( o2 );
        // Single "|" to get both expressions always evaluated, you know, it's a good pattern to
        // call hasNext before next on iterators.
        boolean c1Has;
        boolean c2Has;
        while ( (c1Has = c1.hasNext()) | (c2Has = c2.hasNext()) )
        {
            if ( !c1Has )
            {
                return -1;
            }
            if ( !c2Has )
            {
                return 1;
            }

            int diff = c1.next().compareTo( c2.next() );
            if ( diff != 0 )
            {
                return diff;
            }
            // else continue
        }
        // All elements are comparable with each other
        return 0;
    }

    @SuppressWarnings( "rawtypes" )
    private Iterator<Comparable> comparables( final String string )
    {
        return new PrefetchingIterator<Comparable>()
        {
            private int index;

            @Override
            protected Comparable fetchNextOrNull()
            {
                if ( index >= string.length() )
                {   // End reached
                    return null;
                }

                int startIndex = index;
                char ch = string.charAt( index );
                boolean isNumber = Character.isDigit( ch );
                while ( Character.isDigit( ch ) == isNumber && ++index < string.length() )
                {
                    ch = string.charAt( index );
                }
                String substring = string.substring( startIndex, index );
                return isNumber ? Long.valueOf( substring ) : substring;
            }
        };
    }
}
