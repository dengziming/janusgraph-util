package janusgraph.util.batchimport.unsafe.input.csv;


import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;
import janusgraph.util.batchimport.unsafe.input.reader.CharReadable;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeeker;
import janusgraph.util.batchimport.unsafe.input.reader.Chunker;
import janusgraph.util.batchimport.unsafe.input.reader.Extractors;
import janusgraph.util.batchimport.unsafe.input.reader.Source.Chunk;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeekers;

import java.io.IOException;

import static janusgraph.util.batchimport.unsafe.input.reader.CharSeekers.charSeeker;


/**
 * {@link Chunker} which parses a chunk of entities when calling {@link #nextChunk(Chunk)},
 * injecting them into {@link EagerCsvInputChunk}, which simply hands them out one by one.
 */
public class EagerParserChunker implements Chunker
{
    private final CharSeeker seeker;
    private final CsvInputParser parser;
    private final int chunkSize;
    private final Decorator decorator;

    public EagerParserChunker(CharReadable reader, IdType idType, Header header, Collector badCollector, Extractors extractors,
                              int chunkSize, Configuration config, Decorator decorator )
    {
        this.chunkSize = chunkSize;
        this.decorator = decorator;
        this.seeker = CharSeekers.charSeeker( reader, config, true );
        this.parser = new CsvInputParser( seeker, config.delimiter(), idType, header, badCollector, extractors );
    }

    @Override
    public boolean nextChunk( Chunk chunk ) throws Exception
    {
        InputEntityArray entities = new InputEntityArray( chunkSize );
        InputEntityVisitor decorated = decorator.apply( entities );
        int cursor = 0;
        for ( ; cursor < chunkSize && parser.next( decorated ); cursor++ )
        {   // just loop through and parse
        }

        if ( cursor > 0 )
        {
            ((EagerCsvInputChunk)chunk).initialize( entities.toArray() );
            return true;
        }
        return false;
    }

    @Override
    public long position()
    {
        return seeker.position();
    }

    @Override
    public void close() throws IOException
    {
        parser.close();
    }

    @Override
    public Chunk newChunk()
    {
        throw new UnsupportedOperationException();
    }
}
