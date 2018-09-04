package janusgraph.util.batchimport.unsafe.input.reader;


import java.io.IOException;

/**
 * In a scenario where there's one reader reading chunks of data, handing those chunks to one or
 * more processors (parsers) of that data, this class comes in handy. This pattern allows for
 * multiple {@link BufferedCharSeeker seeker instances}, each operating over one chunk, not transitioning itself
 * into the next.
 */
public class ClosestNewLineChunker extends CharReadableChunker
{
    public ClosestNewLineChunker(CharReadable reader, int chunkSize )
    {
        super( reader, chunkSize );
    }

    /**
     * Fills the given chunk with data from the underlying {@link CharReadable}, up to a good cut-off point
     * in the vicinity of the buffer size.
     *
     * @param chunk {@link Chunk} to read data into.
     * @return the next {@link Chunk} of data, ending with a new-line or not for the last chunk.
     * @throws IOException on reading error.
     */
    @Override
    public synchronized boolean nextChunk( Source.Chunk chunk ) throws IOException
    {
        ChunkImpl into = (ChunkImpl) chunk;
        int offset = fillFromBackBuffer( into.buffer );
        int leftToRead = chunkSize - offset;
        int read = reader.read( into.buffer, offset, leftToRead );
        if ( read == leftToRead )
        {   // Read from reader. We read data into the whole buffer and there seems to be more data left in reader.
            // This means we're most likely not at the end so seek backwards to the last newline character and
            // put the characters after the newline character(s) into the back buffer.
            int newlineOffset = offsetOfLastNewline( into.buffer );
            if ( newlineOffset > -1 )
            {   // We found a newline character some characters back
                read -= storeInBackBuffer( into.data(), newlineOffset + 1, chunkSize - (newlineOffset + 1) );
            }
            else
            {   // There was no newline character, isn't that weird?
                throw new IllegalStateException( "Weird input data, no newline character in the whole buffer " +
                        chunkSize + ", not supported a.t.m." );
            }
        }
        // else we couldn't completely fill the buffer, this means that we're at the end of a data source, we're good.

        if ( read > 0 )
        {
            offset += read;
            position += read;
            into.initialize( offset, reader.sourceDescription() );
            return true;
        }
        return false;
    }

    private static int offsetOfLastNewline( char[] buffer )
    {
        for ( int i = buffer.length - 1; i >= 0; i-- )
        {
            if ( buffer[i] == '\n' )
            {
                return i;
            }
        }
        return -1;
    }
}
