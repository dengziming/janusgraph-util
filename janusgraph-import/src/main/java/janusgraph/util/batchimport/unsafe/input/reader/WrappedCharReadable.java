package janusgraph.util.batchimport.unsafe.input.reader;

import java.io.IOException;
import java.io.Reader;

/**
 * Wraps a {@link Reader} into a {@link CharReadable}.
 */
class WrappedCharReadable extends CharReadable.Adapter
{
    private final long length;
    private final Reader reader;
    private long position;
    private final String sourceDescription;

    WrappedCharReadable(long length, Reader reader )
    {
        this.length = length;
        this.reader = reader;
        sourceDescription = reader.toString();
    }

    @Override
    public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
    {
        buffer.compact( buffer, from );
        buffer.readFrom( reader );
        position += buffer.available();
        return buffer;
    }

    @Override
    public int read( char[] into, int offset, int length ) throws IOException
    {
        int totalRead = 0;
        boolean eof = false;
        while ( totalRead < length )
        {
            int read = reader.read( into, offset + totalRead, length - totalRead );
            if ( read == -1 )
            {
                eof = true;
                break;
            }
            totalRead += read;
        }
        position += totalRead;
        return totalRead == 0 && eof ? -1 : totalRead;
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

    @Override
    public String sourceDescription()
    {
        return sourceDescription;
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public String toString()
    {
        return sourceDescription;
    }
}
