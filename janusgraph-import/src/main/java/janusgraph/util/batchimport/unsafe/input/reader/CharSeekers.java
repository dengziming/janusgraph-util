package janusgraph.util.batchimport.unsafe.input.reader;

import java.io.FileReader;

import static janusgraph.util.batchimport.unsafe.input.reader.Configuration.DEFAULT;
import static janusgraph.util.batchimport.unsafe.input.reader.ThreadAheadReadable.threadAhead;

/**
 * Factory for common {@link CharSeeker} implementations.
 */
public class CharSeekers
{
    private CharSeekers()
    {
    }

    /**
     * Instantiates a {@link BufferedCharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     *
     * @param reader the {@link CharReadable} which is the source of data, f.ex. a {@link FileReader}.
     * @param config {@link Configuration} for the resulting {@link CharSeeker}.
     * @param readAhead whether or not to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     */
    public static CharSeeker charSeeker( CharReadable reader, Configuration config, boolean readAhead )
    {
        if ( readAhead )
        {   // Thread that always has one buffer read ahead
            reader = threadAhead( reader, config.bufferSize() );
        }

        // Give the reader to the char seeker
        return new BufferedCharSeeker( new AutoReadingSource( reader, config.bufferSize() ), config );
    }

    /**
     * Instantiates a {@link BufferedCharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     *
     * @param reader the {@link CharReadable} which is the source of data, f.ex. a {@link FileReader}.
     * @param bufferSize buffer size of the seeker and, if enabled, the read-ahead thread.
     * @param readAhead whether or not to start a {@link ThreadAheadReadable read-ahead thread}
     * which strives towards always keeping one buffer worth of data read and available from I/O when it's
     * time for the {@link BufferedCharSeeker} to read more data.
     * @param quotationCharacter character to interpret quotation character.
     * @return a {@link CharSeeker} with optional {@link ThreadAheadReadable read-ahead} capability.
     */
    public static CharSeeker charSeeker( CharReadable reader, final int bufferSize, boolean readAhead,
            final char quotationCharacter )
    {
        return charSeeker( reader, new Configuration.Overridden( DEFAULT )
        {
            @Override
            public char quotationCharacter()
            {
                return quotationCharacter;
            }

            @Override
            public int bufferSize()
            {
                return bufferSize;
            }
        }, readAhead );
    }
}
