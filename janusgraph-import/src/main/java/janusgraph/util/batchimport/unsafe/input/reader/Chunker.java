package janusgraph.util.batchimport.unsafe.input.reader;


import java.io.Closeable;
import java.io.IOException;

/**
 * Takes a bigger stream of data and chunks it up into smaller chunks. The {@link Source.Chunk chunks} are allocated
 * explicitly and are passed into {@link #nextChunk(Source.Chunk)} to be filled/assigned with data representing
 * next chunk from the stream. This design allows for efficient reuse of chunks when there are multiple concurrent
 * processors, each processing chunks of data.
 */
public interface Chunker extends Closeable
{
    /**
     * @return a new allocated {@link Source.Chunk} which is to be later passed into {@link #nextChunk(Source.Chunk)}
     * to fill it with data. When a {@link Source.Chunk} has been fully processed then it can be passed into
     * {@link #nextChunk(Source.Chunk)} again to get more data.
     */
    Source.Chunk newChunk();

    /**
     * Fills a previously {@link #newChunk() allocated chunk} with data to be processed after completion
     * of this call.
     *
     * @param chunk {@link Source.Chunk} to fill with data.
     * @return {@code true} if at least some amount of data was passed into the given {@link Source.Chunk},
     * otherwise {@code false} denoting the end of the stream.
     * @throws IOException on I/O error.
     */
    boolean nextChunk(Source.Chunk chunk) throws Exception;

    /**
     * @return byte position of how much data has been returned from {@link #nextChunk(Source.Chunk)}.
     */
    long position();
}
