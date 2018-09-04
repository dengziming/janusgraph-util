package janusgraph.util.batchimport.unsafe.helps.collection;

/**
 * Package-private static methods shared between Primitive, PrimitiveIntCollections and PrimitiveLongCollections
 */
class PrimitiveCommons
{
    private PrimitiveCommons()
    {
    }

    /**
     * If the given obj is AutoCloseable, then close it.
     * Any exceptions thrown from the close method will be wrapped in RuntimeExceptions.
     */
    static void closeSafely( Object obj )
    {
        closeSafely( obj, null );
    }

    /**
     * If the given obj is AutoCloseable, then close it.
     * Any exceptions thrown from the close method will be wrapped in RuntimeExceptions.
     * These RuntimeExceptions can get the given suppressedException attached to them, if any.
     * If the given suppressedException argument is null, then it will not be added to the
     * thrown RuntimeException.
     */
    static void closeSafely( Object obj, Throwable suppressedException )
    {
        if ( obj instanceof AutoCloseable )
        {
            AutoCloseable closeable = (AutoCloseable) obj;
            try
            {
                closeable.close();
            }
            catch ( Exception cause )
            {
                RuntimeException exception = new RuntimeException( cause );
                if ( suppressedException != null )
                {
                    exception.addSuppressed( suppressedException );
                }
                throw exception;
            }
        }
    }
}
