package janusgraph.util.batchimport.unsafe.input;

import java.util.function.Supplier;

/**
 * {@link Iterable} that returns {@link InputIterator} instances.
 */
public interface InputIterable
{
    InputIterator iterator();

    /**
     * @return whether or not multiple calls to {@link #iterator()} and therefore multiple passes
     * over its data is supported.
     */
    boolean supportsMultiplePasses();

    static InputIterable replayable(Supplier<InputIterator> source)
    {
        return new InputIterable()
        {
            @Override
            public InputIterator iterator()
            {
                return source.get();
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }
}
