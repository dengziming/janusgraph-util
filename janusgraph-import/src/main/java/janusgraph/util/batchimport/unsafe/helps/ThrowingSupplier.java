package janusgraph.util.batchimport.unsafe.helps;

import java.util.function.Supplier;

/**
 * Represents a supplier of results, that may throw an exception.
 *
 * @param <T> the type of results supplied by this supplier
 * @param <E> the type of exception that may be thrown from the function
 */
public interface ThrowingSupplier<T, E extends Exception>
{
    /**
     * Gets a result.
     *
     * @return A result
     * @throws E an exception if the function fails
     */
    T get() throws E;

    static <TYPE> ThrowingSupplier<TYPE,RuntimeException> throwingSupplier(Supplier<TYPE> supplier)
    {
        return new ThrowingSupplier<TYPE,RuntimeException>()
        {
            @Override
            public TYPE get()
            {
                return supplier.get();
            }

            @Override
            public String toString()
            {
                return supplier.toString();
            }
        };
    }
}
