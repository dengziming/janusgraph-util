package janusgraph.util.batchimport.unsafe.input;


import janusgraph.util.batchimport.unsafe.helps.ResourceIterator;

import java.io.Closeable;
import java.io.IOException;

/**
 * A {@link ResourceIterator}
 * with added methods suitable for {@link Input}
 * into a {@link BatchImporter}.
 */
public interface InputIterator extends Closeable {
    InputChunk newChunk();

    boolean next(InputChunk chunk) throws Exception;

    abstract class Adapter implements InputIterator
    {
        @Override
        public void close() throws IOException
        {   // Nothing to close
        }
    }

    class Delegate implements InputIterator
    {
        protected final InputIterator actual;

        public Delegate( InputIterator actual )
        {
            this.actual = actual;
        }

        @Override
        public void close() throws IOException
        {
            actual.close();
        }

        @Override
        public InputChunk newChunk()
        {
            return actual.newChunk();
        }

        @Override
        public boolean next( InputChunk chunk ) throws Exception
        {
            return actual.next( chunk );
        }
    }

    class Empty extends Adapter
    {
        @Override
        public InputChunk newChunk()
        {
            return InputChunk.EMPTY;
        }

        @Override
        public boolean next( InputChunk chunk )
        {
            return false;
        }
    }
}
