package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.raddix;

import janusgraph.util.batchimport.unsafe.helps.Factory;
import org.apache.commons.lang3.mutable.MutableInt;

import static java.lang.Math.pow;

/**
 * Calculates and keeps radix counts. Uses a {@link RadixCalculator} to calculate an integer radix value
 * from a long value.
 */
public abstract class Radix
{
    public static final Factory<Radix> LONG = Long::new;

    public static final Factory<Radix> STRING = String::new;

    protected final int[] radixIndexCount = new int[(int) pow( 2, RadixCalculator.RADIX_BITS - 1 )];

    public int registerRadixOf( long value )
    {
        int radix = calculator().radixOf( value );
        radixIndexCount[radix]++;
        return radix;
    }

    public int[] getRadixIndexCounts()
    {
        return radixIndexCount;
    }

    public abstract RadixCalculator calculator();

    @Override
    public java.lang.String toString()
    {
        return Radix.class.getSimpleName() + "." + getClass().getSimpleName();
    }

    public static class String extends Radix
    {
        private final RadixCalculator calculator;

        public String()
        {
            this.calculator = new RadixCalculator.String();
        }

        @Override
        public RadixCalculator calculator()
        {
            return calculator;
        }
    }

    public static class Long extends Radix
    {
        private final MutableInt radixShift;
        private final RadixCalculator calculator;

        public Long()
        {
            this.radixShift = new MutableInt();
            this.calculator = new RadixCalculator.Long( radixShift );
        }

        @Override
        public RadixCalculator calculator()
        {
            return calculator;
        }

        @Override
        public int registerRadixOf( long value )
        {
            radixOverflow( value );
            return super.registerRadixOf( value );
        }

        private void radixOverflow( long val )
        {
            long shiftVal = (val & ~RadixCalculator.LENGTH_BITS) >> (RadixCalculator.RADIX_BITS - 1 + radixShift.intValue());
            if ( shiftVal > 0 )
            {
                while ( shiftVal > 0 )
                {
                    radixShift.increment();
                    compressRadixIndex();
                    shiftVal = shiftVal >> 1;
                }
            }
        }

        private void compressRadixIndex()
        {
            for ( int i = 0; i < radixIndexCount.length / 2; i++ )
            {
                radixIndexCount[i] = radixIndexCount[2 * i] + radixIndexCount[2 * i + 1];
            }
            for ( int i = radixIndexCount.length / 2; i < radixIndexCount.length; i++ )
            {
                radixIndexCount[i] = 0;
            }
        }
    }
}
