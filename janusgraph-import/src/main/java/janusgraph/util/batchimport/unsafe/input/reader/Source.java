package janusgraph.util.batchimport.unsafe.input.reader;

import java.io.Closeable;
import java.io.IOException;

/**
 * Source of data chunks to read.
 */
public interface Source extends Closeable
{
    Chunk nextChunk(int seekStartPos) throws IOException;

    /**
     * One chunk of data to read.
     */
    interface Chunk
    {
        /**
         * @return character data to read
         */
        char[] data();

        /**
         * @return number of effective characters in the {@link #data()}
         */
        int length();

        /**
         * @return effective capacity of the {@link #data()} array
         */
        int maxFieldSize();

        /**
         * @return source description of the source this chunk was read from
         */
        String sourceDescription();

        /**
         * @return position in the {@link #data()} array to start reading from
         */
        int startPosition();

        /**
         * @return position in the {@link #data()} array where the current field which is being
         * read starts. Some characters of the current field may have started in the previous chunk
         * and so those characters are transfered over to this data array before {@link #startPosition()}
         */
        int backPosition();
    }

    Chunk EMPTY_CHUNK = new Chunk()
    {
        @Override
        public int startPosition()
        {
            return 0;
        }

        @Override
        public String sourceDescription()
        {
            return "EMPTY";
        }

        @Override
        public int maxFieldSize()
        {
            return 0;
        }

        @Override
        public int length()
        {
            return 0;
        }

        @Override
        public char[] data()
        {
            return null;
        }

        @Override
        public int backPosition()
        {
            return 0;
        }
    };

    static Source singleChunk(Chunk chunk)
    {
        return new Source()
        {
            private boolean returned;

            @Override
            public void close() throws IOException
            {   // Nothing to close
            }

            @Override
            public Chunk nextChunk( int seekStartPos ) throws IOException
            {
                if ( !returned )
                {
                    returned = true;
                    return chunk;
                }
                return EMPTY_CHUNK;
            }
        };
    }
}
