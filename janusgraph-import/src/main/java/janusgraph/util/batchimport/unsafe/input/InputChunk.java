package janusgraph.util.batchimport.unsafe.input;

import java.io.Closeable;
import java.io.IOException;

/**
 * A chunk of data which an {@link InputEntityVisitor} can visit to extract data from. There may be zero or
 * more entities in a chunk.
 */
public interface InputChunk extends Closeable
{
    InputChunk EMPTY = new InputChunk()
    {
        @Override
        public boolean next( InputEntityVisitor visitor ) throws IOException
        {
            return false;
        }

        @Override
        public void close() throws IOException
        {
        }
    };

    boolean next(InputEntityVisitor visitor) throws Exception;
}
