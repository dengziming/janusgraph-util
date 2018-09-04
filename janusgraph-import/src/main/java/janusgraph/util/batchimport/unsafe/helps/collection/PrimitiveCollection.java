package janusgraph.util.batchimport.unsafe.helps.collection;

public interface PrimitiveCollection extends AutoCloseable
{
    boolean isEmpty();

    void clear();

    int size();

    /**
     * Free any attached resources.
     */
    @Override
    void close();
}
