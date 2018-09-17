package janusgraph.util.batchimport.unsafe.input.csv;

import janusgraph.util.batchimport.unsafe.input.Input;

/**
 * Factory for the {@link Data data} provided by an {@link Input}.
 */
public interface DataFactory
{
    Data create(Configuration config);
}
