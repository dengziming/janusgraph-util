package janusgraph.util.batchimport.unsafe.input.csv;

/**
 * Factory for the {@link Data data} provided by an {@link Input}.
 */
public interface DataFactory
{
    Data create(Configuration config);
}
