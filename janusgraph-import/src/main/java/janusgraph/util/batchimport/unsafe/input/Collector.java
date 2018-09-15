package janusgraph.util.batchimport.unsafe.input;

/**
 * Collects items and is {@link #close() closed} after any and all items have been collected.
 * The {@link Collector} is responsible for closing whatever closeable resource received from the importer.
 */
public interface Collector extends AutoCloseable
{
    void collectBadEdge(
            Object startId, String startIdGroup, String type,
            Object endId, String endIdGroup, Object specificValue) throws Exception;

    void collectDuplicateNode(Object id, long actualId, String group) throws Exception;

    void collectExtraColumns(String source, long row, String value) throws Exception;

    long badEntries();

    boolean isCollectingBadEdges();

    /**
     * Flushes whatever changes to the underlying resource supplied from the importer.
     */
    @Override
    void close();

    Collector EMPTY = new Collector()
    {
        @Override
        public void collectExtraColumns( String source, long row, String value )
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public long badEntries()
        {
            return 0;
        }

        @Override
        public void collectBadEdge(Object startId, String startIdGroup, String type, Object endId, String endIdGroup,
                                   Object specificValue )
        {
        }

        @Override
        public void collectDuplicateNode( Object id, long actualId, String group )
        {
        }

        @Override
        public boolean isCollectingBadEdges()
        {
            return true;
        }
    };
}
