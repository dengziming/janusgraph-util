package janusgraph.util.batchimport.unsafe.helps;

import java.util.Optional;
import java.util.function.Function;

/**
 * Represents a function that accepts one argument and produces a result, or throws an exception.
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 * @param <E> the type of exception that may be thrown from the function
 */
public interface ThrowingFunction<T, R, E extends Exception>
{
    /**
     * Apply a value to this function
     *
     * @param t the function argument
     * @return the function result
     * @throws E an exception if the function fails
     */
    R apply(T t) throws E;

    /**
     * Construct a regular function that calls a throwing function and catches all checked exceptions
     * declared and thrown by the throwing function and rethrows them as {@link UncaughtCheckedException}
     * for handling further up the stack.
     *
     * @see UncaughtCheckedException
     *
     * @param throwing the throwing function to wtap
     * @param <T> type of arguments
     * @param <R> type of results
     * @param <E> type of checked exceptions thrown by the throwing function
     * @return a new, non-throwing function
     * @throws IllegalStateException if an unexpected exception is caught (ie. neither of type E or a runtime exception)
     */
    static <T, R, E extends Exception> Function<T, R> catchThrown(Class<E> clazz, ThrowingFunction<T, R, E> throwing)
    {
        return input ->
        {
            try
            {
                return throwing.apply( input );
            }
            catch ( Exception e )
            {
                if ( clazz.isInstance( e ) )
                {
                    throw new UncaughtCheckedException( throwing, clazz.cast( e ) );
                }
                else if ( e instanceof RuntimeException )
                {
                    throw (RuntimeException) e;
                }
                else
                {
                    throw new IllegalStateException( "Unexpected exception", e );
                }
            }
        };
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    static <E extends Exception> void throwIfPresent(Optional<E> exception) throws E
    {
        if ( exception.isPresent() )
        {
            throw exception.get();
        }
    }
}
