package janusgraph.util.batchimport.unsafe.stage;

import static java.lang.Math.round;

/**
 * Takes a value range and projects it to a very discrete number of integer values, quantizing based
 * on float precision.
 */
public class QuantizedProjection
{
    private final long max;
    private final long projectedMax;

    private double absoluteWay;
    private long step;

    public QuantizedProjection(long max, long projectedMax )
    {
        this.max = max;
        this.projectedMax = projectedMax;
    }

    /**
     * @param step a part of the max, not the projection.
     * @return {@code true} if the total so far including {@code step} is equal to or less than the max allowed,
     * otherwise {@code false} -- meaning that we stepped beyond max.
     */
    public boolean next( long step )
    {
        double absoluteStep = (double)step / (double)max;
        if ( absoluteWay + absoluteStep > 1f )
        {
            return false;
        }

        long prevProjection = round( absoluteWay * projectedMax );
        absoluteWay += absoluteStep;
        long projection = round( absoluteWay * projectedMax );
        this.step = projection - prevProjection;

        return true;
    }

    public long step()
    {
        return step;
    }
}
