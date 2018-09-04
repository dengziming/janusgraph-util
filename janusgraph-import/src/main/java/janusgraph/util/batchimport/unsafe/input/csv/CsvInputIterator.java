package janusgraph.util.batchimport.unsafe.input.csv;

import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Groups;
import janusgraph.util.batchimport.unsafe.input.reader.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import static janusgraph.util.batchimport.unsafe.input.csv.CsvGroupInputIterator.extractors;
import static java.util.Arrays.copyOf;


/**
 * Iterates over one stream of data, where all data items conform to the same {@link Header}.
 * Typically created from {@link CsvGroupInputIterator}.
 */
class CsvInputIterator implements SourceTraceability, Closeable
{
    private final CharReadable stream;
    private final Chunker chunker;
    private final int groupId;
    private final Decorator decorator;
    private final Supplier<CsvInputChunk> realInputChunkSupplier;

    CsvInputIterator(CharReadable stream, Decorator decorator, Header header, Configuration config, IdType idType, Collector badCollector,
                     Extractors extractors, int groupId )
    {
        this.stream = stream;
        this.decorator = decorator;
        this.groupId = groupId;
        if ( config.multilineFields() )
        {
            // If we're expecting multi-line fields then there's no way to arbitrarily chunk the underlying data source
            // and find record delimiters with certainty. This is why we opt for a chunker that does parsing inside
            // the call that normally just hands out an arbitrary amount of characters to parse outside and in parallel.
            // This chunker is single-threaded, as it was previously too and keeps the functionality of multi-line fields.
            this.chunker = new EagerParserChunker( stream, idType, header, badCollector, extractors, 1_000, config, decorator );
            this.realInputChunkSupplier = EagerCsvInputChunk::new;
        }
        else
        {
            this.chunker = new ClosestNewLineChunker( stream, config.bufferSize() );
            this.realInputChunkSupplier = () -> new LazyCsvInputChunk( idType, config.delimiter(), badCollector,
                    extractors( config ), chunker.newChunk(), config, decorator, header );
        }
    }

    CsvInputIterator( CharReadable stream, Decorator decorator, Header.Factory headerFactory, IdType idType, Configuration config, Groups groups,
            Collector badCollector, Extractors extractors, int groupId ) throws IOException
    {
        this( stream, decorator, extractHeader( stream, headerFactory, idType, config, groups ), config, idType, badCollector, extractors, groupId );
    }

    static Header extractHeader( CharReadable stream, Header.Factory headerFactory, IdType idType,
            Configuration config, Groups groups ) throws IOException
    {
        if ( !headerFactory.isDefined() )
        {
            char[] firstLineBuffer = Readables.extractFirstLineFrom( stream );
            // make the chunk slightly bigger than the header to not have the seeker think that it's reading
            // a value bigger than its max buffer size
            CharReadableChunker.ChunkImpl firstChunk = new CharReadableChunker.ChunkImpl( copyOf( firstLineBuffer, firstLineBuffer.length + 1 ) );
            firstChunk.initialize( firstLineBuffer.length, stream.sourceDescription() );
            CharSeeker firstSeeker = seeker( firstChunk, config );
            return headerFactory.create( firstSeeker, config, idType, groups );
        }

        return headerFactory.create( null, null, null, null );
    }

    public boolean next( CsvInputChunkProxy proxy ) throws Exception
    {
        proxy.ensureInstantiated( realInputChunkSupplier, groupId );
        return proxy.fillFrom( chunker );
    }

    @Override
    public void close() throws IOException
    {
        chunker.close();
        decorator.close();
    }

    @Override
    public String sourceDescription()
    {
        return stream.sourceDescription();
    }

    @Override
    public long position()
    {
        return chunker.position();
    }

    static CharSeeker seeker(Source.Chunk chunk, Configuration config )
    {
        return new BufferedCharSeeker( Source.singleChunk( chunk ), config );
    }
}
