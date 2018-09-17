package janusgraph.util.batchimport.unsafe;


import janusgraph.util.batchimport.unsafe.input.Input;

/**
 * Imports graph data given as {@link Input}.
 */
public interface BatchImporter
{
    void doImport(Input input)
            throws Exception;
}
