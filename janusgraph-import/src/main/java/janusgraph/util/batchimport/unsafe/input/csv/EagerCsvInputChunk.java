package janusgraph.util.batchimport.unsafe.input.csv;



import janusgraph.util.batchimport.unsafe.input.InputEntity;
import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;
import janusgraph.util.batchimport.unsafe.input.reader.Chunker;
import janusgraph.util.batchimport.unsafe.input.reader.Source;

import java.io.IOException;

class EagerCsvInputChunk implements CsvInputChunk, Source.Chunk
{
    private InputEntity[] entities;
    private int cursor;

    void initialize( InputEntity[] entities )
    {
        this.entities = entities;
        this.cursor = 0;
    }

    @Override
    public boolean next( InputEntityVisitor visitor ) throws IOException
    {
        if ( cursor < entities.length )
        {
            entities[cursor++].replayOnto( visitor );
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public boolean fillFrom( Chunker chunker ) throws Exception
    {
        return chunker.nextChunk( this );
    }

    @Override
    public char[] data()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int length()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxFieldSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String sourceDescription()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int startPosition()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int backPosition()
    {
        throw new UnsupportedOperationException();
    }
}
