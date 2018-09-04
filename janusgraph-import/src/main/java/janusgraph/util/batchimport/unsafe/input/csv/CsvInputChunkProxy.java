package janusgraph.util.batchimport.unsafe.input.csv;



import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;
import janusgraph.util.batchimport.unsafe.input.reader.Chunker;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * {@link CsvInputChunk} that adapts new input source groups during the streaming of data.
 * {@link InputIterator} is fairly straight-forward, but is made a bit more complicated by the fact that
 * there can be multiple different data streams. The outer iterator, {@link CsvGroupInputIterator}, is still responsible
 * for handing out chunks, something that generally is good thing since it solves a bunch of other problems.
 * The problem it has is that it doesn't know exactly which type of {@link CsvInputChunk} it wants to create,
 * because that's up to each input group. This gap is bridged here in this class.
 */
public class CsvInputChunkProxy implements CsvInputChunk
{
    private CsvInputChunk actual;
    private int groupId = -1;

    public void ensureInstantiated( Supplier<CsvInputChunk> newChunk, int groupId ) throws IOException
    {
        if ( actual == null || groupId != this.groupId )
        {
            closeCurrent();
            actual = newChunk.get();
        }
        this.groupId = groupId;
    }

    @Override
    public void close() throws IOException
    {
        closeCurrent();
    }

    private void closeCurrent() throws IOException
    {
        if ( actual != null )
        {
            actual.close();
        }
    }

    @Override
    public boolean fillFrom( Chunker chunker ) throws Exception
    {
        return actual.fillFrom( chunker );
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws Exception
    {
        return actual.next( visitor );
    }
}
