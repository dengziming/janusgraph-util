package janusgraph.util.batchimport.unsafe.io;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;

/**
 * IO helper methods.
 */
public final class IOUtils
{
    private IOUtils()
    {
    }

    /**
     * Closes given {@link Collection collection} of {@link AutoCloseable closeables}.
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @throws IOException
     * @see #closeAll(AutoCloseable[])
     */
    public static <T extends AutoCloseable> void closeAll( Collection<T> closeables ) throws IOException
    {
        closeAll( closeables.toArray( new AutoCloseable[closeables.size()] ) );
    }

    /**
     * Closes given array of {@link AutoCloseable closeables}. If any {@link AutoCloseable#close()} call throws
     * {@link IOException} than it will be rethrown to the caller after calling {@link AutoCloseable#close()}
     * on other given resources. If more than one {@link AutoCloseable#close()} throw than resulting exception will
     * have suppressed exceptions. See {@link Exception#addSuppressed(Throwable)}
     *
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @throws IOException
     */
    @SafeVarargs
    public static <T extends AutoCloseable> void closeAll( T... closeables ) throws IOException
    {
        closeAll( IOException.class, closeables );
    }

    /**
     * Close all given closeables and if something goes wrong throw exception of the given type.
     * Exception class should have a public constructor that accepts {@link String} and {@link Throwable} like
     * {@link RuntimeException#RuntimeException(String, Throwable)}
     *
     * @param throwableClass exception type to throw in case of failure
     * @param closeables the closeables to close
     * @param <T> the type of closeable
     * @param <E> the type of exception
     * @throws E when any {@link AutoCloseable#close()} throws exception
     */
    @SafeVarargs
    public static <T extends AutoCloseable, E extends Throwable> void closeAll( Class<E> throwableClass,
            T... closeables ) throws E
    {
        Throwable closeThrowable = null;
        for ( T closeable : closeables )
        {
            if ( closeable != null )
            {
                try
                {
                    closeable.close();
                }
                catch ( Throwable t )
                {
                    if ( closeThrowable == null )
                    {
                        closeThrowable = t;
                    }
                    else
                    {
                        closeThrowable.addSuppressed( t );
                    }
                }
            }
        }
        if ( closeThrowable != null )
        {
            throw newThrowable( throwableClass, "Exception closing multiple resources", closeThrowable );
        }
    }

    private static <E extends Throwable> E newThrowable( Class<E> throwableClass, String message, Throwable cause )
    {
        try
        {
            Constructor<E> constructor = throwableClass.getConstructor( String.class, Throwable.class );
            return constructor.newInstance( message, cause );
        }
        catch ( Throwable t )
        {
            RuntimeException runtimeException = new RuntimeException(
                    "Unable to create exception to throw. Original message: " + message, t );
            runtimeException.addSuppressed( cause );
            throw runtimeException;
        }
    }
}
