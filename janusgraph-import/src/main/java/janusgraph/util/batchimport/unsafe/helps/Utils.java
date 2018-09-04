package janusgraph.util.batchimport.unsafe.helps;

/**
 * Common and cross-concern utilities.
 */
public class Utils
{
    public enum CompareType
    {
        EQ, GT, GE, LT, LE, NE
    }

    public static boolean unsignedCompare( long dataA, long dataB, CompareType compareType )
    {   // works for signed and unsigned values
        switch ( compareType )
        {
        case EQ:
            return dataA == dataB;
        case GE:
            if ( dataA == dataB )
            {
                return true;
            }
            // fall through to GT
        case GT:
            return dataA < dataB == ((dataA < 0) != (dataB < 0));
        case LE:
            if ( dataA == dataB )
            {
                return true;
            }
            // fall through to LT
        case LT:
            return (dataA < dataB) ^ ((dataA < 0) != (dataB < 0));
        case NE:
            return false;

        default:
            throw new IllegalArgumentException( "Unknown compare type: " + compareType );
        }
    }

    /**
     * Like {@link #unsignedCompare(long, long, CompareType)} but reversed in that you get {@link CompareType}
     * from comparing data A and B, i.e. the difference between them.
     */
    public static CompareType unsignedDifference( long dataA, long dataB )
    {
        if ( dataA == dataB )
        {
            return CompareType.EQ;
        }
        return ((dataA < dataB) ^ ((dataA < 0) != (dataB < 0))) ? CompareType.LT : CompareType.GT;
    }

    // Values in the arrays are assumed to be sorted
    public static boolean anyIdCollides( long[] first, int firstLength, long[] other, int otherLength )
    {
        int f = 0;
        int o = 0;
        while ( f < firstLength && o < otherLength )
        {
            if ( first[f] == other[o] )
            {
                return true;
            }

            if ( first[f] < other[o] )
            {
                while ( ++f < firstLength && first[f] < other[o] )
                {
                }
            }
            else
            {
                while ( ++o < otherLength && first[f] > other[o] )
                {
                }
            }
        }

        return false;
    }

    public static void mergeSortedInto( long[] values, long[] into, int intoLengthBefore )
    {
        int v = values.length - 1;
        int i = intoLengthBefore - 1;
        int t = i + values.length;
        while ( v >= 0 || i >= 0 )
        {
            if ( i == -1 )
            {
                into[t--] = values[v--];
            }
            else if ( v == -1 )
            {
                into[t--] = into[i--];
            }
            else if ( values[v] >= into[i] )
            {
                into[t--] = values[v--];
            }
            else
            {
                into[t--] = into[i--];
            }
        }
    }

    private Utils()
    {   // No instances allowed
    }
}
