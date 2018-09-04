package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.raddix;

import janusgraph.util.batchimport.unsafe.idmapper.Encoder;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Calculates the radix of {@link Long} values.
 */
public abstract class RadixCalculator
{
    protected static final int RADIX_BITS = 24;
    protected static final long LENGTH_BITS = 0xFE000000_00000000L;
    protected static final int LENGTH_MASK = (int) (LENGTH_BITS >>> (64 - RADIX_BITS));
    protected static final int HASHCODE_MASK = (int) (0x00FFFF00_00000000L >>> (64 - RADIX_BITS));

    public abstract int radixOf( long value );

    /**
     * Radix optimized for strings encoded into long by {@link Encoder}.
     */
    public static class String extends RadixCalculator
    {
        @Override
        public int radixOf( long value )
        {
            int index = (int) (value >>> (64 - RADIX_BITS));
//            java.lang.String s = Integer.toBinaryString(index );
//            java.lang.String s0 = Integer.toBinaryString(index & LENGTH_MASK);
//            java.lang.String s1 = Integer.toBinaryString((index & LENGTH_MASK ) >>> 1);
//            java.lang.String s2 = Integer.toBinaryString(index & HASHCODE_MASK);
            index = ((index & LENGTH_MASK) >>> 1) | (index & HASHCODE_MASK);
//            java.lang.String s4 = Integer.toBinaryString(index);
            return index;
        }
    }

    /**
     * Radix optimized for strings encoded into long by {@link Encoder}.
     */
    public static class Long extends RadixCalculator
    {
        private final MutableInt radixShift;

        public Long( MutableInt radixShift )
        {
            this.radixShift = radixShift;
        }

        @Override
        public int radixOf( long value )
        {
            long val1 = value & ~LENGTH_BITS;
            val1 = val1 >>> radixShift.intValue();
            int index = (int) val1;
            return index;
        }
    }
}
