package janusgraph.util.batchimport.unsafe.input.csv;



import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Groups;
import janusgraph.util.batchimport.unsafe.input.InputChunk;
import janusgraph.util.batchimport.unsafe.input.InputIterator;
import janusgraph.util.batchimport.unsafe.input.reader.Extractors;
import janusgraph.util.batchimport.unsafe.input.reader.MultiReadable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

/**
 * Iterates over groups of input data, each group containing one or more input files. A whole group conforms has each its own header.
 */
public class CsvGroupInputIterator extends InputIterator.Adapter
{
    private final Iterator<DataFactory> source;
    private final Header.Factory headerFactory;
    private final IdType idType;
    private final Configuration config;
    private final Collector badCollector;
    private final Groups groups;
    private CsvInputIterator current;
    private int groupId;

    public CsvGroupInputIterator( Iterator<DataFactory> source, Header.Factory headerFactory,
            IdType idType, Configuration config, Collector badCollector, Groups groups )
    {
        this.source = source;
        this.headerFactory = headerFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;
        this.groups = groups;
    }

    @Override
    public CsvInputChunkProxy newChunk()
    {
        return new CsvInputChunkProxy();
    }

    static Extractors extractors(Configuration config )
    {
        return new Extractors( config.arrayDelimiter(), config.emptyQuotedStringsAsNull() );
    }

    @Override
    public synchronized boolean next( InputChunk chunk ) throws Exception
    {
        while ( true )
        {
            if ( current == null )
            {
                if ( !source.hasNext() )
                {
                    return false;
                }
                Data data = source.next().create( config );
                current = new CsvInputIterator( new MultiReadable( data.stream() ), data.decorator(),
                        headerFactory, idType, config, groups, badCollector, extractors( config ), groupId++ );
            }

            if ( current.next( (CsvInputChunkProxy) chunk ) )
            {
                return true;
            }
            current.close();
            current = null;
        }
    }

    @Override
    public void close()
    {
        try
        {
            if ( current != null )
            {
                current.close();
            }
            current = null;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
