package janusgraph.util.batchimport.unsafe.input.reader;


import java.io.IOException;
import java.util.Arrays;

/**
 * Chunks up a {@link CharReadable}.
 */
public abstract class CharReadableChunker implements Chunker
{
    protected final CharReadable reader;
    protected final int chunkSize;
    protected volatile long position;
    private char[] backBuffer; // grows on demand
    private int backBufferCursor;

    public CharReadableChunker(CharReadable reader, int chunkSize )
    {
        this.reader = reader;
        this.chunkSize = chunkSize;
        this.backBuffer = new char[chunkSize >> 4];
    }

    @Override
    public ChunkImpl newChunk()
    {
        return new ChunkImpl( new char[chunkSize] );
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }

    @Override
    public long position()
    {
        return position;
    }

    protected int fillFromBackBuffer( char[] into )
    {
        if ( backBufferCursor > 0 )
        {   // Read from and reset back buffer
            assert backBufferCursor < chunkSize;
            System.arraycopy( backBuffer, 0, into, 0, backBufferCursor );
            int result = backBufferCursor;
            backBufferCursor = 0;
            return result;
        }
        return 0;
    }

    protected int storeInBackBuffer( char[] data, int offset, int length )
    {
        System.arraycopy( data, offset, backBuffer( length ), backBufferCursor, length );
        backBufferCursor += length;
        return length;
    }

    private char[] backBuffer( int length )
    {
        if ( backBufferCursor + length > backBuffer.length )
        {
            backBuffer = Arrays.copyOf( backBuffer, backBufferCursor + length );
        }
        return backBuffer;
    }

    public static class ChunkImpl implements Source.Chunk
    {
        final char[] buffer;
        private int length;
        private String sourceDescription;

        public ChunkImpl( char[] buffer )
        {
            this.buffer = buffer;
        }

        public void initialize( int length, String sourceDescription )
        {
            this.length = length;
            this.sourceDescription = sourceDescription;
        }

        @Override
        public int startPosition()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return sourceDescription;
        }

        @Override
        public int maxFieldSize()
        {
            return buffer.length;
        }

        @Override
        public int length()
        {
            return length;
        }

        @Override
        public char[] data()
        {
            return buffer;
        }

        @Override
        public int backPosition()
        {
            return 0;
        }
    }
}
