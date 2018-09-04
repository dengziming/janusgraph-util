package janusgraph.util.batchimport.unsafe.stage;

import java.time.Clock;

/**
 * This class consists of {@code static} utility methods for operating
 * on clocks. These utilities include factory methods for different type of clocks.
 */
public class Clocks
{
    private static final Clock SYSTEM_CLOCK = Clock.systemUTC();

    private Clocks()
    {
        // non-instantiable
    }

    /**
     * Returns system clock.
     * @return system clock
     */
    public static Clock systemClock()
    {
        return SYSTEM_CLOCK;
    }

}
