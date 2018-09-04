package janusgraph.util.batchimport.unsafe.helps;

import java.util.function.Predicate;

/**
 * Represents a predicate (boolean-valued function) of one argument.
 *
 * @param <T> the type of the input to the predicate
 * @param <E> the type of exception that may be thrown from the operator
 */
public interface ThrowingPredicate<T, E extends Exception>
{
    /**
     * Evaluates this predicate on the given argument.
     *
     * @param t the input argument
     * @return true if the input argument matches the predicate, otherwise false
     * @throws E an exception if the predicate fails
     */
    boolean test(T t) throws E;

    static <TYPE> ThrowingPredicate<TYPE,RuntimeException> throwingPredicate(Predicate<TYPE> predicate)
    {
        return new ThrowingPredicate<TYPE,RuntimeException>()
        {
            @Override
            public boolean test( TYPE value )
            {
                return predicate.test( value );
            }

            @Override
            public String toString()
            {
                return predicate.toString();
            }
        };
    }
}
