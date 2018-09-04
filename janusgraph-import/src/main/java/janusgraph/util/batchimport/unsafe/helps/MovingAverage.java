package janusgraph.util.batchimport.unsafe.helps;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static java.lang.Math.min;

/**
 * In a moving average calculation, only the last N values are considered.
 */
public class MovingAverage
{
    private final AtomicLongArray values;
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong valueCursor = new AtomicLong();

    public MovingAverage(int numberOfTrackedValues )
    {
        this.values = new AtomicLongArray( numberOfTrackedValues );
    }

    public void add( long value )
    {
        long cursor = valueCursor.getAndIncrement();
        long prevValue = values.getAndSet( (int) (cursor % values.length()), value );
        total.addAndGet( value - prevValue );
    }

    private int numberOfCurrentlyTrackedValues()
    {
        return (int) min( valueCursor.get(), values.length() );
    }

    public long total()
    {
        return total.get();
    }

    public long average()
    {
        int trackedValues = numberOfCurrentlyTrackedValues();
        return trackedValues > 0 ? total.get() / trackedValues : 0;
    }

    public void reset()
    {
        for ( int i = 0; i < values.length(); i++ )
        {
            values.set( i, 0 );
        }
        total.set( 0 );
        valueCursor.set( 0 );
    }
}
