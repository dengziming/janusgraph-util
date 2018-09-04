package janusgraph.util.batchimport.unsafe.input.reader;

import java.io.IOException;

/**
 * In a scenario where there's one thread, or perhaps a {@link ThreadAheadReadable} doing both the
 * reading and parsing one {@link BufferedCharSeeker} is used over a stream of chunks, where the next
 * chunk seamlessly transitions into the next, this class comes in handy. It uses a {@link CharReadable}
 * and {@link SectionedCharBuffer} to do this.
 */
public class AutoReadingSource implements Source
{
    private final CharReadable reader;
    private SectionedCharBuffer charBuffer;

    public AutoReadingSource(CharReadable reader, int bufferSize )
    {
        this( reader, new SectionedCharBuffer( bufferSize ) );
    }

    public AutoReadingSource(CharReadable reader, SectionedCharBuffer charBuffer )
    {
        this.reader = reader;
        this.charBuffer = charBuffer;
    }

    @Override
    public Chunk nextChunk( int seekStartPos ) throws IOException
    {
        charBuffer = reader.read( charBuffer, seekStartPos == -1 ? charBuffer.pivot() : seekStartPos );

        return new Chunk()
        {
            @Override
            public int startPosition()
            {
                return charBuffer.pivot();
            }

            @Override
            public String sourceDescription()
            {
                return reader.sourceDescription();
            }

            @Override
            public int backPosition()
            {
                return charBuffer.back();
            }

            @Override
            public int length()
            {
                return charBuffer.available();
            }

            @Override
            public int maxFieldSize()
            {
                return charBuffer.pivot();
            }

            @Override
            public char[] data()
            {
                return charBuffer.array();
            }
        };
    }

    @Override
    public void close() throws IOException
    {
        reader.close();
    }
}
