package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.IntArray;
import janusgraph.util.batchimport.unsafe.idmapper.cache.LongBitsManipulator;

/**
 * {@link Tracker} capable of keeping {@code int} range values, using {@link IntArray}.
 * Will fail in {@link #set(long, long)} with {@link ArithmeticException} if trying to put a too big value.
 */
public class IntTracker extends AbstractTracker<IntArray>
{
    public static final int SIZE = Integer.BYTES;
    public static final int ID_BITS = Byte.SIZE * SIZE - 1;
    public static final long MAX_ID = (1 << ID_BITS) - 1;
    public static final int DEFAULT_VALUE = -1;
    private static final LongBitsManipulator BITS = new LongBitsManipulator( ID_BITS, 1 );

    public IntTracker( IntArray array )
    {
        super( array );
    }

    @Override
    public long get( long index )
    {
        return BITS.get( array.get( index ), 0 );
    }

    /**
     * @throws ArithmeticException if value is bigger than {@link Integer#MAX_VALUE}.
     */
    @Override
    public void set( long index, long value )
    {
        long field = array.get( index );
        field = BITS.set( field, 0, value );
        array.set( index, (int) field );
    }

    @Override
    public void markAsDuplicate( long index )
    {
        long field = array.get( index );
        // Since the default value for the whole field is -1 (i.e. all 1s) then this mark will have to be 0.
        field = BITS.set( field, 1, 0 );
        array.set( index, (int) field );
    }

    @Override
    public boolean isMarkedAsDuplicate( long index )
    {
        long field = array.get( index );
        return BITS.get( field, 1 ) == 0;
    }
}
