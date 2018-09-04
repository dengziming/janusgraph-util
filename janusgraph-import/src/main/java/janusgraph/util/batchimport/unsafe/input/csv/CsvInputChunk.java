package janusgraph.util.batchimport.unsafe.input.csv;



import janusgraph.util.batchimport.unsafe.input.InputChunk;
import janusgraph.util.batchimport.unsafe.input.reader.Chunker;

import java.io.IOException;

/**
 * {@link InputChunk} that gets data from {@link Chunker}. Making it explicit in the interface simplifies implementation
 * where there are different types of {@link Chunker} for different scenarios.
 */
public interface CsvInputChunk extends InputChunk
{
    /**
     * Fills this {@link InputChunk} from the given {@link Chunker}.
     *
     * @param chunker to read next chunk from.
     * @return {@code true} if there was data read, otherwise {@code false}, meaning end of stream.
     * @throws IOException on I/O read error.
     */
    boolean fillFrom(Chunker chunker) throws Exception;
}
