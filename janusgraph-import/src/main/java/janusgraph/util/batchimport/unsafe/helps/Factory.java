package janusgraph.util.batchimport.unsafe.helps;

/**
 * A generic factory interface for creating instances that do not need any additional dependencies or parameters.
 * If the implementation is not always creating new instances, you should probably use
 * {@link java.util.function.Supplier}.
 *
 * @param <T> a new instance
 */
public interface Factory<T>
{
    T newInstance();
}
