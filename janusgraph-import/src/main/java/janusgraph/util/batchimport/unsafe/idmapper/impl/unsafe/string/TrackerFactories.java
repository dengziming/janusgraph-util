package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;

/**
 * Common {@link TrackerFactory} implementations.
 */
public class TrackerFactories
{
    private TrackerFactories()
    {
    }

    /**
     * @return {@link TrackerFactory} creating different {@link Tracker} instances depending on size.
     */
    public static TrackerFactory dynamic()
    {
        return ( arrayFactory, size ) -> size > IntTracker.MAX_ID
                ? new BigIdTracker( arrayFactory.newByteArray( size, BigIdTracker.DEFAULT_VALUE ) )
                : new IntTracker( arrayFactory.newIntArray( size, IntTracker.DEFAULT_VALUE ) );
    }
}
