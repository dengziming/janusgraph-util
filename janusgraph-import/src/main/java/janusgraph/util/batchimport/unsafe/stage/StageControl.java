package janusgraph.util.batchimport.unsafe.stage;

import java.util.function.Supplier;

/**
 * Represents a means to control and coordinate lifecycle matters about a {@link Stage} and all its
 * {@link Step steps}.
 */
public interface StageControl
{
    void panic(Throwable cause);

    void assertHealthy();

    void recycle(Object batch);

    <T> T reuse(Supplier<T> fallback);
}
