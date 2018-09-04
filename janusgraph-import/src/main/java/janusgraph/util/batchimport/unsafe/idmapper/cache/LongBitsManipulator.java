package janusgraph.util.batchimport.unsafe.idmapper.cache;


import janusgraph.util.batchimport.unsafe.helps.Bits;

import java.util.Arrays;

/**
 * Turns a long into 64 bits of memory where variables can be allocated in, for example:
 * <pre>
 * [eeee,eeee][dddd,dddd][dddd,dddd][dddd,cccc][bbbb,bbbb][bbbb,bbbb][bbaa,aaaa][aaaa,aaaa]
 * </pre>
 * Which has the variables a (14 bits), b (18 bits), c (4 bits), d (20 bits) and e (8 bits)
 */
public class LongBitsManipulator
{
    private static class Slot
    {
        private final long mask;
        private final long maxValue;
        private final int bitOffset;

        Slot( int bits, long mask, int bitOffset ) // 32,1 111111 (31个1),100000 (31个0) 0,32
        {
            this.mask = mask;// 1111111111 (32个1)
            this.bitOffset = bitOffset;// 0,31
            this.maxValue = (1L << bits) - 1;
        }

        public long get( long field )
        {
            long raw = field & mask;
            return raw == mask ? -1 : raw >>> bitOffset;
        }

        public long set( long field, long value )
        {
            if ( value < -1 || value > maxValue )
            {
                throw new IllegalStateException( "Invalid value " + value + ", max is " + maxValue );
            }

            long otherBits = field & ~mask;
            return ((value << bitOffset) & mask) | otherBits;
        }

        public long clear( long field, boolean trueForAllOnes )
        {
            long otherBits = field & ~mask;
            return trueForAllOnes ?
                    // all bits in this slot as 1
                    otherBits | mask :

                    // all bits in this slot as 0
                    otherBits;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + Bits.numbersToBitString( new long[] {maxValue << bitOffset} ) + "]";
        }
    }

    private final Slot[] slots;

    public LongBitsManipulator(int... slotsAndTheirBitCounts )
    {
        slots = intoSlots( slotsAndTheirBitCounts );// 31,1
    }

    private Slot[] intoSlots( int[] slotsAndTheirSizes )
    {
        Slot[] slots = new Slot[slotsAndTheirSizes.length];
        int bitCursor = 0;
        for ( int i = 0; i < slotsAndTheirSizes.length; i++ )
        {
            int bits = slotsAndTheirSizes[i];
            long mask = (1L << bits) - 1;// 111111 (31个1), 1
            mask <<= bitCursor;// 111111 (33个1),100000 (32个0)
            slots[i] = new Slot( bits, mask, bitCursor );
            bitCursor += bits;
        }
        return slots;
    }

    public int slots()
    {
        return slots.length;
    }

    public long set( long field, int slotIndex, long value )
    {
        return slot( slotIndex ).set( field, value );
    }

    public long get( long field, int slotIndex )
    {
        return slot( slotIndex ).get( field );
    }

    public long clear( long field, int slotIndex, boolean trueForAllOnes )
    {
        return slot( slotIndex ).clear( field, trueForAllOnes );
    }

    public long template( boolean... trueForOnes )
    {
        if ( trueForOnes.length != slots.length )
        {
            throw new IllegalArgumentException( "Invalid boolean arguments, expected " + slots.length );
        }

        long field = 0;
        for ( int i = 0; i < trueForOnes.length; i++ )
        {
            field = slots[i].clear( field, trueForOnes[i] );
        }
        return field;
    }

    private Slot slot( int slotIndex )
    {
        if ( slotIndex < 0 || slotIndex >= slots.length )
        {
            throw new IllegalArgumentException( "Invalid slot " + slotIndex + ", I've got " + this );
        }
        return slots[slotIndex];
    }

    @Override
    public String toString()
    {
        return Arrays.toString( slots );
    }
}
