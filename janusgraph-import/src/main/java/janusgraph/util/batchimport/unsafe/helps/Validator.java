package janusgraph.util.batchimport.unsafe.helps;

/**
 * Validates a value, throws {@link IllegalArgumentException} for invalid values.
 */
public interface Validator<T>
{
    void validate(T value);
}
