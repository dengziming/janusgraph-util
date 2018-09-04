package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.ByteArray;
import janusgraph.util.batchimport.unsafe.idmapper.cache.LongBitsManipulator;

import java.util.Arrays;

/**
 * {@link Tracker} capable of keeping 6B range values, using {@link ByteArray}.
 */
public class BigIdTracker extends AbstractTracker<ByteArray>
{
    public static final int SIZE = 5;
    public static final int ID_BITS = (Byte.SIZE * SIZE) - 1;
    public static final byte[] DEFAULT_VALUE;
    public static final long MAX_ID = 1L << ID_BITS - 1;
    private static final LongBitsManipulator BITS = new LongBitsManipulator( ID_BITS, 1 );
    static
    {
        DEFAULT_VALUE = new byte[SIZE];
        Arrays.fill( DEFAULT_VALUE, (byte) -1 );
    }

    public BigIdTracker( ByteArray array )
    {
        super( array );
    }

    @Override
    public long get( long index )
    {
        return BITS.get( array.get5ByteLong( index, 0 ), 0 );
    }

    @Override
    public void set( long index, long value )
    {
        long field = array.get5ByteLong( index, 0 );
        field = BITS.set( field, 0, value );
        array.set5ByteLong( index, 0, field );
    }

    @Override
    public void markAsDuplicate( long index )
    {
        long field = array.get5ByteLong( index, 0 );
        field = BITS.set( field, 1, 0 );
        array.set5ByteLong( index, 0, field );
    }

    @Override
    public boolean isMarkedAsDuplicate( long index )
    {
        long field = array.get5ByteLong( index, 0 );
        return BITS.get( field, 1 ) == 0;
    }
}
