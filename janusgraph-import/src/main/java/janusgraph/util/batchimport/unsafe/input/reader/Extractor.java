package janusgraph.util.batchimport.unsafe.input.reader;

/**
 * Extracts a value from a part of a {@code char[]} into any type of value, f.ex. a {@link Extractors#string()},
 * {@link Extractors#long_() long} or {@link Extractors#intArray()}.
 *
 * An {@link Extractor} is mutable for the single purpose of ability to reuse its value instance. Consider extracting
 * a primitive int -
 *
 * Sub-interfaces and implementations can and should specify specific accessors for the purpose
 * of performance and less garbage, f.ex. where an IntExtractor could have an accessor method for
 * getting the extracted value as primitive int, to avoid auto-boxing which would arise from calling {@link #value()}.
 *
 * @see Extractors for a collection of very common extractors.
 */
public interface Extractor<T> extends Cloneable
{
    /**
     * Extracts value of type {@code T} from the given character data.
     * @param data characters in a buffer.
     * @param offset offset into the buffer where the value starts.
     * @param length number of characters from the offset to extract.
     * @param hadQuotes whether or not there were skipped characters, f.ex. quotation.
     * @return {@code true} if a value was extracted, otherwise {@code false}.
     */
    boolean extract(char[] data, int offset, int length, boolean hadQuotes);

    /**
     * @return the most recently extracted value.
     */
    T value();

    /**
     * @return string representation of what type of value of produces. Also used as key in {@link Extractors}.
     */
    String name();

    Extractor<T> clone();
}
