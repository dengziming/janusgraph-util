package janusgraph.util.batchimport.unsafe.input.csv;

import janusgraph.util.batchimport.unsafe.helps.Resource;
import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;

import java.util.function.Function;

public interface Decorator extends Function<InputEntityVisitor,InputEntityVisitor>, Resource
{
    /**
     * @return whether or not this decorator is mutable. This is important because a state-less decorator
     * can be called from multiple parallel processing threads. A mutable decorator has to be called by
     * a single thread and may incur a performance penalty.
     */
    default boolean isMutable()
    {
        return false;
    }

    @Override
    default void close()
    {   // Nothing to close by default
    }
}
