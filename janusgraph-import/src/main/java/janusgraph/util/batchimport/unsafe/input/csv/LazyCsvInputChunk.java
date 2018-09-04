package janusgraph.util.batchimport.unsafe.input.csv;


import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.InputChunk;
import janusgraph.util.batchimport.unsafe.input.InputEntityVisitor;
import janusgraph.util.batchimport.unsafe.input.reader.Chunker;
import janusgraph.util.batchimport.unsafe.input.reader.Extractors;
import janusgraph.util.batchimport.unsafe.input.reader.Source;

import java.io.IOException;


/**
 * {@link InputChunk} parsing next entry on each call to {@link #next(InputEntityVisitor)}.
 */
public class LazyCsvInputChunk implements CsvInputChunk
{
    private final IdType idType;
    private final int delimiter;
    private final Collector badCollector;
    private final Source.Chunk processingChunk;
    private final Configuration config;
    private final Decorator decorator;
    private final Header header;
    private final Extractors extractors;

    // Set in #fillFrom
    private CsvInputParser parser;

    // Set as #next is called
    private InputEntityVisitor previousVisitor;
    private InputEntityVisitor visitor;

    public LazyCsvInputChunk(IdType idType, int delimiter, Collector badCollector, Extractors extractors, Source.Chunk processingChunk, Configuration config,
                             Decorator decorator, Header header )
    {
        this.idType = idType;
        this.badCollector = badCollector;
        this.extractors = extractors;
        this.delimiter = delimiter;
        this.processingChunk = processingChunk;
        this.config = config;
        this.decorator = decorator;
        this.header = header;
    }

    @Override
    public boolean fillFrom( Chunker chunker ) throws Exception
    {
        if ( chunker.nextChunk( processingChunk ) )
        {
            closeCurrentParser();
            this.visitor = null;
            this.parser = new CsvInputParser( CsvInputIterator.seeker( processingChunk, config ), delimiter, idType, header.clone(), badCollector, extractors );
            return header.entries().length != 0;
        }
        return false;
    }

    private void closeCurrentParser() throws IOException
    {
        if ( parser != null )
        {
            parser.close();
        }
    }

    @Override
    public boolean next( InputEntityVisitor nakedVisitor ) throws Exception
    {
        if ( visitor == null || nakedVisitor != previousVisitor )
        {
            decorateVisitor( nakedVisitor );
        }

        return parser.next( visitor );
    }

    private void decorateVisitor( InputEntityVisitor nakedVisitor )
    {
        visitor = decorator.apply( nakedVisitor );
        previousVisitor = nakedVisitor;
    }

    @Override
    public void close() throws IOException
    {
        closeCurrentParser();
    }
}
