package janusgraph.util.batchimport.unsafe.io.fs;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;

public interface StoreChannel
        extends Flushable, SeekableByteChannel, GatheringByteChannel, ScatteringByteChannel, InterruptibleChannel
{

    /**
     * @see FileChannel#read(ByteBuffer, long)
     */
    int read(ByteBuffer dst, long position) throws IOException;

    void force(boolean metaData) throws IOException;

    @Override
    StoreChannel position(long newPosition) throws IOException;

    @Override
    StoreChannel truncate(long size) throws IOException;
}
