package janusgraph.util.batchimport.unsafe.input.csv;

import janusgraph.util.batchimport.unsafe.graph.GraphUtil;
import janusgraph.util.batchimport.unsafe.helps.collection.RawIterator;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.input.*;
import janusgraph.util.batchimport.unsafe.input.reader.CharReadable;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeeker;
import janusgraph.util.batchimport.unsafe.input.reader.MultiReadable;
import janusgraph.util.batchimport.unsafe.helps.ByteUnit;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeekers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.ToIntFunction;

import static janusgraph.util.batchimport.unsafe.input.Collector.EMPTY;
import static janusgraph.util.batchimport.unsafe.input.InputIterable.replayable;
import static janusgraph.util.batchimport.unsafe.input.csv.CsvGroupInputIterator.extractors;
import static janusgraph.util.batchimport.unsafe.input.reader.CharSeekers.charSeeker;

/**
 * Provides {@link Input} from data contained in tabular/csv form. Expects factories for instantiating
 * the {@link CharSeeker} objects seeking values in the csv data and header factories for how to
 * extract meta data about the values.
 */
public class CsvInput implements Input
{
    private static final long ESTIMATE_SAMPLE_SIZE = ByteUnit.mebiBytes( 1 );

    private final Iterable<DataFactory> nodeDataFactory;
    private final Header.Factory nodeHeaderFactory;
    private final Iterable<DataFactory> relationshipDataFactory;
    private final Header.Factory relationshipHeaderFactory;
    private final IdType idType;
    private final Configuration config;
    private final Collector badCollector;
    private final Groups groups;

    /**
     * @param nodeDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code nodeHeaderFactory}. From the outside
     * it looks like one stream of nodes.
     * @param nodeHeaderFactory factory for reading node headers.
     * @param relationshipDataFactory multiple {@link DataFactory} instances providing data, each {@link DataFactory}
     * specifies an input group with its own header, extracted by the {@code relationshipHeaderFactory}.
     * From the outside it looks like one stream of relationships.
     * @param relationshipHeaderFactory factory for reading relationship headers.
     * @param idType {@link IdType} to expect in id fields of node and relationship input.
     * @param config CSV configuration.
     * @param badCollector Collector getting calls about bad input data.
     */
    public CsvInput(
            Iterable<DataFactory> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config, Collector badCollector )
    {
        this( nodeDataFactory, nodeHeaderFactory, relationshipDataFactory, relationshipHeaderFactory, idType, config, badCollector,
                new Groups() );
    }

    CsvInput(
            Iterable<DataFactory> nodeDataFactory, Header.Factory nodeHeaderFactory,
            Iterable<DataFactory> relationshipDataFactory, Header.Factory relationshipHeaderFactory,
            IdType idType, Configuration config, Collector badCollector, Groups groups )
    {
        assertSaneConfiguration( config );

        this.nodeDataFactory = nodeDataFactory;
        this.nodeHeaderFactory = nodeHeaderFactory;
        this.relationshipDataFactory = relationshipDataFactory;
        this.relationshipHeaderFactory = relationshipHeaderFactory;
        this.idType = idType;
        this.config = config;
        this.badCollector = badCollector;
        this.groups = groups;

        verifyHeaders();
    }

    /**
     * Verifies so that all headers in input files looks sane:
     * <ul>
     * <li>node/relationship headers can be parsed correctly</li>
     * <li>relationship headers uses ID spaces previously defined in node headers</li>
     * </ul>
     */
    private void verifyHeaders()
    {
        try
        {
            // parse all node headers and remember all ID spaces
            for ( DataFactory dataFactory : nodeDataFactory )
            {
                try ( CharSeeker dataStream = CharSeekers.charSeeker( new MultiReadable( dataFactory.create( config ).stream() ), config, true ) )
                {
                    // Parsing and constructing this header will create this group,
                    // so no need to do something with the result of it right now
                    Header header = nodeHeaderFactory.create(dataStream, config, idType, groups);
                    GraphUtil.createSchema(header);
                }
            }

            // parse all relationship headers and verify all ID spaces
            for ( DataFactory dataFactory : relationshipDataFactory )
            {
                try ( CharSeeker dataStream = CharSeekers.charSeeker( new MultiReadable( dataFactory.create( config ).stream() ), config, true ) )
                {
                    // Merely parsing and constructing the header here will as a side-effect verify that the
                    // id groups already exists (relationship header isn't allowed to create groups)
                    Header header = relationshipHeaderFactory.create(dataStream, config, idType, groups);
                    GraphUtil.createSchema(header);
                }
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static void assertSaneConfiguration( Configuration config )
    {
        Map<Character,String> delimiters = new HashMap<>();
        delimiters.put( config.delimiter(), "delimiter" );
        checkUniqueCharacter( delimiters, config.arrayDelimiter(), "array delimiter" );
        checkUniqueCharacter( delimiters, config.quotationCharacter(), "quotation character" );
    }

    private static void checkUniqueCharacter( Map<Character,String> characters, char character, String characterDescription )
    {
        String conflict = characters.put( character, characterDescription );
        if ( conflict != null )
        {
            throw new IllegalArgumentException( "Character '" + character + "' specified by " + characterDescription +
                    " is the same as specified by " + conflict );
        }
    }

    @Override
    public InputIterable nodes()
    {
        return replayable( () -> stream( nodeDataFactory, nodeHeaderFactory ) );
    }

    @Override
    public InputIterable relationships()
    {
        return replayable( () -> stream( relationshipDataFactory, relationshipHeaderFactory ) );
    }

    private InputIterator stream(Iterable<DataFactory> data, Header.Factory headerFactory )
    {
        return new CsvGroupInputIterator( data.iterator(), headerFactory, idType, config, badCollector, groups );
    }

    @Override
    public IdMapper idMapper(NumberArrayFactory numberArrayFactory )
    {
        return idType.idMapper( numberArrayFactory, groups );
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    @Override
    public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator ) throws Exception
    {
        long[] nodeSample = sample( nodeDataFactory, nodeHeaderFactory, valueSizeCalculator, node -> node.labels().length );
        long[] relationshipSample = sample( relationshipDataFactory, relationshipHeaderFactory, valueSizeCalculator, entity -> 0 );
        return Inputs.knownEstimates(
                nodeSample[0], relationshipSample[0],
                nodeSample[1], relationshipSample[1],
                nodeSample[2], relationshipSample[2],
                nodeSample[3] );
    }

    private long[] sample( Iterable<DataFactory> dataFactories, Header.Factory headerFactory,
            ToIntFunction<Value[]> valueSizeCalculator, ToIntFunction<InputEntity> additionalCalculator ) throws Exception
    {
        long[] estimates = new long[4]; // [entity count, property count, property size, labels (for nodes only)]
        try ( CsvInputChunkProxy chunk = new CsvInputChunkProxy() )
        {
            // One group of input files
            int groupId = 0;
            for ( DataFactory dataFactory : dataFactories ) // one input group
            {
                groupId++;
                Header header = null;
                Data data = dataFactory.create( config );
                RawIterator<CharReadable,IOException> sources = data.stream();
                while ( sources.hasNext() )
                {
                    try ( CharReadable source = sources.next() )
                    {
                        if ( header == null )
                        {
                            // Extract the header from the first file in this group
                            header = CsvInputIterator.extractHeader( source, headerFactory, idType, config, groups );
                        }
                        try ( CsvInputIterator iterator = new CsvInputIterator( source, data.decorator(), header, config,
                                idType, EMPTY, extractors( config ), groupId );
                              InputEntity entity = new InputEntity() )
                        {
                            int entities = 0;
                            int properties = 0;
                            int propertySize = 0;
                            int additional = 0;
                            while ( iterator.position() < ESTIMATE_SAMPLE_SIZE && iterator.next( chunk ) )
                            {
                                for ( ; chunk.next( entity ); entities++ )
                                {
                                    properties += entity.propertyCount();
                                    propertySize += Inputs.calculatePropertySize( entity, valueSizeCalculator );
                                    additional += additionalCalculator.applyAsInt( entity );
                                }
                            }
                            long entityCount = entities > 0 ? (long) (((double) source.length() / iterator.position()) * entities) : 0;
                            estimates[0] += entityCount;
                            estimates[1] += ((double) properties / entities) * entityCount;
                            estimates[2] += ((double) propertySize / entities) * entityCount;
                            estimates[3] += ((double) additional / entities) * entityCount;
                        }
                    }
                }
            }
        }
        return estimates;
    }
}
