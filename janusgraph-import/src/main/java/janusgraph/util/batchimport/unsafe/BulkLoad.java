package janusgraph.util.batchimport.unsafe;

import janusgraph.util.batchimport.unsafe.graph.GraphUtil;
import janusgraph.util.batchimport.unsafe.graph.store.StoreManager;
import janusgraph.util.batchimport.unsafe.graph.store.proxy.ProxyManager;
import janusgraph.util.batchimport.unsafe.helps.*;
import janusgraph.util.batchimport.unsafe.helps.collection.IterableWrapper;
import janusgraph.util.batchimport.unsafe.helps.collection.RawIterator;
import janusgraph.util.batchimport.unsafe.input.*;
import janusgraph.util.batchimport.unsafe.input.csv.*;
import janusgraph.util.batchimport.unsafe.input.reader.CharReadable;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeeker;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeekers;
import janusgraph.util.batchimport.unsafe.input.reader.Readables;
import janusgraph.util.batchimport.unsafe.io.IOUtils;
import janusgraph.util.batchimport.unsafe.io.fs.FileSystem;
import janusgraph.util.batchimport.unsafe.io.fs.FileSystemImpl;
import janusgraph.util.batchimport.unsafe.io.fs.FileUtils;
import janusgraph.util.batchimport.unsafe.lifecycle.CombineLifecycle;
import janusgraph.util.batchimport.unsafe.load.BulkLoader;
import janusgraph.util.batchimport.unsafe.load.ProxyBulkLoader;
import janusgraph.util.batchimport.unsafe.log.LogService;
import janusgraph.util.batchimport.unsafe.log.StoreLogService;
import janusgraph.util.batchimport.unsafe.stage.ExecutionMonitors;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.Function;

import static janusgraph.util.batchimport.unsafe.BulkLoad.Options.RECREATE_DATABASE_IF_EXISTS;
import static janusgraph.util.batchimport.unsafe.helps.Exceptions.launderedException;
import static janusgraph.util.batchimport.unsafe.helps.MetaDataStore.DEFAULT_NAME;
import static janusgraph.util.batchimport.unsafe.helps.TextUtil.tokenizeStringWithQuotes;
import static janusgraph.util.batchimport.unsafe.input.Collectors.badCollector;
import static janusgraph.util.batchimport.unsafe.input.Collectors.silentBadCollector;
import static java.lang.Character.isDigit;
import static java.lang.Long.parseLong;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Arrays.asList;

/**
 * bulk loading tools
 * a tool to batch load data into janusgrpah, but this tool is not safe,which means:
 * <ul>
 *     <li>this can be used when init, if there are already data, this can't be used</li>
 *     <li>this is not thread safe</li>
 *     <li>this is not transaction, which means this doesn't promise ACID</li>
 *     <li>No constraint is imposed when inserting data</li>
 *     <li>if a error occur, the data is unusable</li>
 *     <li>just import nodes and edges, index will not take efforts, you should use janusgraphManagement to do it </li>
 * </ul>
 *
 * to use this tool, steps are as follows:
 * <ol>
 *     <li>prepare your data in format of CSV, (if in other format, you can implement your own Parser) </li>
 *     <li>node files should have a primary key and label, and properties if have</li>
 *     <li>edge data should have two primary key from node, and properties if have </li>
 *     <li>use java command to import data</li>
 *     <li>build index use janusgraph management</li>
 * </ol>
 * owing to the bulk will use some core api instead of thrift api, so if you are using cassandra,
 * you should make sure the cassandra embed with janusgraph are the same version with your cassandra cluster.
 *
 * for example, we have some data as {@code GraphOfTheGodsFactory}, we can write them in some files
 *
 *    v_titan.txt
 *    name:ID(titan),age:Int
 *    saturn,10000

 *    v_location.csv
 *    name:ID(location)
 *    sky
 *    sea
 *    tartarus

 *    v_god.csv
 *    name:ID(god),age:Int
 *    jupiter,5000
 *    neptune,4500
 *    pluto,4000

 *    v_demigod.csv
 *    name:ID(demigod),age:Int
 *    hercules,30

 *    v_human.csv
 *    name:ID(human),age:Int
 *    alcmene,45

 *    v_monster.csv
 *    name:ID(monster)
 *    nemean
 *    hydra
 *    cerberus


 *    e_god_titan_father.csv
 *    :START_ID(god),:END_ID(titan)
 *    jupiter,saturn

 *    e_demigod_god_father.csv
 *    :START_ID(demigod),:END_ID(god)
 *    hercules,jupiter

 *    e_demigod_human_father.csv
 *    :START_ID(demigod),:END_ID(human)
 *    hercules,alcmene

 *    e_god_location_lives.csv
 *    :START_ID(god),:END_ID(location),reason
 *    jupiter,sky,loves fresh breezes
 *    neptune,sea,loves waves
 *    pluto,tartarus,no fear of death

 *    e_monster_location_lives.csv
 *    :START_ID(monster),:END_ID(location),reason
 *    cerberus,tartarus,

 *    e_god_god_brother.csv
 *    :START_ID(god),:END_ID(god)
 *    jupiter,neptune
 *    jupiter,pluto
 *    neptune,jupiter
 *    neptune,pluto
 *    pluto,neptune
 *    pluto,jupiter

 *    e_demigod_monster_battled.csv
 *    :START_ID(demigod),:END_ID(monster),time:Int,place
 *    hercules,nemean,1,38.1f_23.7f
 *    hercules,hydra,2,37.7f_23.9f
 *    hercules,cerberus,12,39f_22f

 *    e_god_monster_pet.csv
 *    :START_ID(god),:END_ID(monster)
 *    pluto,cerberus
 *
 * and then we use the following commend to load it into database:
 *    java -cp class.path.to.BulkLoad
 *    --into /path/to/sstable/dir \
 *    --skip-duplicate-nodes true \
 *    --skip-bad-edges true \
 *    --ignore-extra-columns true \
 *    --ignore-empty-strings true \
 *    --bad-tolerance 10000000 \
 *    --processors 1 \
 *    --id-type string \
 *    --max-memory 2G \
 *    --nodes:titan v_titan.csv \
 *    --nodes:location v_location.csv \
 *    --nodes:god v_god.csv \
 *    --nodes:demigod v_demigod.csv \
 *    --nodes:human v_human.csv \
 *    --nodes:monster v_monster.csv \
 *    --edges:father e_god_titan_father.txt \
 *    --edges:father e_demigod_god_father.txt \
 *    --edges:mother e_demigod_human_mother.txt \
 *    --edges:lives e_god_location_lives.txt \
 *    --edges:lives e_monster_location_lives.txt \
 *    --edges:brother e_god_god_brother.txt \
 *    --edges:battled e_demigod_monster_battled.txt \
 *    --edges:pet e_god_monster_pet.txt \
 *    > /path/to/logfile.log
 *
 * and the you can use gremilin or janusgraph api to query:
 *
 * gremlin> g.V().hasLabel("titan").valueMap()
 * ==>[name:[saturn],age:[10000]]
 *
 * gremlin> saturn = g.V().has("name", "saturn").next();
 * ==>v[4096]
 * gremlin> g.V(saturn).valueMap()
 * ==>[name:[saturn],age:[10000]]
 *
 * gremlin> g.V(saturn).in("father").in("father").values("name")
 * ==>hercules
 *
 * gremlin> g.V(saturn).in("father").values()
 * ==>jupiter
 * ==>5000
 *
 */
public class BulkLoad {

    /**
     * Delimiter used between files in an input group.
     */
    static final String MULTI_FILE_DELIMITER = ",";
    private static final Function<String,IdType> TO_ID_TYPE = from -> IdType.valueOf( from.toUpperCase() );
    private static final Function<String,Character> CHARACTER_CONVERTER = new CharacterConverter();
    public static <T> Function<String,T> withDefault( final T defaultValue )
    {
        return from -> defaultValue;
    }


    public static void main(String[] args0) throws Exception {

        Args args = Args.parse( args0 );
        PrintStream err = System.err;
        PrintStream out = System.out;

        Collection<Args.Option<File[]>> nodesFiles;
        Collection<Args.Option<File[]>> edgesFiles;
        Charset inputEncoding;
        Collector badCollector;
        IdType idType;



        if ( ArrayUtil.isEmpty( args0 ) || asksForUsage( args ) )
        {
            printUsage( out );
            return;
        }

        File storeDir;
        Number processors;
        long badTolerance;
        boolean skipBadEdges;
        boolean enableStacktrace;
        boolean skipDuplicateNodes;
        boolean ignoreExtraColumns;
        boolean skipBadEntriesLogging;
        OutputStream badOutput = null;
        Configuration configuration;
        File logsDir;
        File badFile = null;
        Long maxMemory;
        Boolean defaultHighIO;
        InputStream in;

        boolean success = false;
        try ( FileSystem fs = new FileSystemImpl() )
        {
            args = useArgumentsFromFileArgumentIfPresent( args );

            storeDir = args.interpretOption( Options.STORE_DIR.key(), Converters.mandatory(),
                    Converters.toFile(), Validators.DIRECTORY_IS_WRITABLE, Validators.CONTAINS_NO_EXISTING_DATABASE );

            logsDir = new File(storeDir,"logs");

            skipBadEntriesLogging = args.getBoolean( Options.SKIP_BAD_ENTRIES_LOGGING.key(),
                    (Boolean) Options.SKIP_BAD_ENTRIES_LOGGING.defaultValue(), false);
            if ( !skipBadEntriesLogging )
            {
                badFile = new File( storeDir, Configuration.BAD_FILE_NAME );
                badOutput = new BufferedOutputStream( fs.openAsOutputStream( badFile, false ) );
            }
            nodesFiles = extractInputFiles( args, Options.NODE_DATA.key(), err );
            edgesFiles = extractInputFiles( args, Options.EDGE_DATA.key(), err );
            String maxMemoryString = args.get( Options.MAX_MEMORY.key(), null );
            maxMemory = parseMaxMemory( maxMemoryString );

            validateInputFiles( nodesFiles, edgesFiles );
            enableStacktrace = args.getBoolean( Options.STACKTRACE.key(), Boolean.FALSE, Boolean.TRUE );
            processors = args.getNumber( Options.PROCESSORS.key(), null );
            idType = args.interpretOption( Options.ID_TYPE.key(),
                    withDefault( (IdType) Options.ID_TYPE.defaultValue() ), TO_ID_TYPE );
            badTolerance = parseNumberOrUnlimited( args, Options.BAD_TOLERANCE );
            inputEncoding = Charset.forName( args.get( Options.INPUT_ENCODING.key(), defaultCharset().name() ) );

            skipBadEdges = args.getBoolean( Options.SKIP_BAD_EDGES.key(),
                    (Boolean) Options.SKIP_BAD_EDGES.defaultValue(), true );
            skipDuplicateNodes = args.getBoolean( Options.SKIP_DUPLICATE_NODES.key(),
                    (Boolean) Options.SKIP_DUPLICATE_NODES.defaultValue(), true );
            ignoreExtraColumns = args.getBoolean( Options.IGNORE_EXTRA_COLUMNS.key(),
                    (Boolean) Options.IGNORE_EXTRA_COLUMNS.defaultValue(), true );
            defaultHighIO = args.getBoolean( Options.HIGH_IO.key(),
                    (Boolean) Options.HIGH_IO.defaultValue(), true );

            badCollector = getBadCollector( badTolerance, skipBadEdges, skipDuplicateNodes, ignoreExtraColumns,
                    skipBadEntriesLogging, badOutput );

            boolean allowCacheOnHeap = args.getBoolean( Options.CACHE_ON_HEAP.key(),
                    (Boolean) Options.CACHE_ON_HEAP.defaultValue() );
            configuration = importConfiguration(
                    processors, maxMemory, storeDir,
                    allowCacheOnHeap, defaultHighIO );

            String janus_config_file = args.get(Options.JANUS_CONFIG_FILE.key());
            StandardJanusGraph graph = getGraph(janus_config_file,extractJanusConfig(args, Options.JANUS_CONFIG.key(), err));

            // drop already exists keyspace
            if (args.getBoolean(RECREATE_DATABASE_IF_EXISTS.key())){
                // then we should drop
                out.println("drop database if exists");
                StoreManager manager = new ProxyManager(graph);
                manager.reCreateGraphIfExists();
                GraphUtil.close();
                //
                graph = getGraph(janus_config_file,extractJanusConfig(args, Options.JANUS_CONFIG.key(), err));
            }

            CsvInput input = new CsvInput( nodeData( inputEncoding, nodesFiles ), DataFactories.defaultFormatNodeFileHeader(),
                    edgeData( inputEncoding, edgesFiles ), DataFactories.defaultFormatEdgeFileHeader(),
                    idType, csvConfiguration( args ,false), badCollector );
            in = System.in;
            doImport( graph, out, err, in, storeDir, logsDir, badFile, fs, nodesFiles, edgesFiles,
                    enableStacktrace, input, badOutput, configuration );

            success = true;

            // TODO to move this to import in parallel
            // import to cassandra

            out.println();
            BulkLoader loader = new ProxyBulkLoader(graph);
            loader.load(graph,out,err,in,storeDir,logsDir,configuration);
        }
        catch ( IllegalArgumentException e )
        {
            throw andPrintError( "Input error", e, false, err );
        }
        catch ( IOException e )
        {
            throw andPrintError( "File error", e, false, err );
        }
        finally
        {
            if ( !success && badOutput != null )
            {
                badOutput.close();
            }
            GraphUtil.close();
        }
    }

    public static void doImport(StandardJanusGraph graph,PrintStream out, PrintStream err, InputStream in, File storeDir, File logsDir, File badFile,
                                FileSystem fs, Collection<Args.Option<File[]>> nodesFiles,
                                Collection<Args.Option<File[]>> edgesFiles, boolean enableStacktrace, Input input,
                                OutputStream badOutput,
                                Configuration configuration ) throws IOException
    {
        boolean success;
        CombineLifecycle life = new CombineLifecycle();

        File flag = new File(storeDir,DEFAULT_NAME);
        if (storeDir.exists()){
            storeDir.mkdirs();
        }
        flag.createNewFile();

        File internalLogFile = new File(logsDir,"debug.log");
        LogService logService = life.add( StoreLogService.withInternalLog( internalLogFile ).build( fs ) );

        life.start();

        BatchImporter importer = BatchImporterFactory.getInstance().instantiate(
                storeDir,
                fs,
                configuration,
                logService,
                ExecutionMonitors.defaultVisible( in ),
                new PrintingImportLogicMonitor( out, err ) ,
                graph,
                GraphUtil.getIdAssigner());
        out.println("Create VertexLabel and EdgeLabel");
        GraphUtil.createVertexLabel(nodesFiles);
        GraphUtil.createEdgeLabel(edgesFiles);

        printOverview( storeDir, nodesFiles, edgesFiles, configuration, out );
        success = false;
        try
        {
            out.println("Generate SSTable Files");
            importer.doImport( input );
            success = true;
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw andPrintError( "Import error", e, enableStacktrace, err );
        }
        finally
        {
            Collector collector = input.badCollector();
            long numberOfBadEntries = collector.badEntries();
            collector.close();
            IOUtils.closeAll( badOutput );

            if ( badFile != null )
            {
                if ( numberOfBadEntries > 0 )
                {
                    System.out.println( "There were bad entries which were skipped and logged into " +
                            badFile.getAbsolutePath() );
                }
            }

            life.shutdown();

            if ( !success )
            {
                err.println( "WARNING Import failed. The store files in " + storeDir.getAbsolutePath() +
                        " are left as they are, although they are likely in an unusable state. " );
            }
        }
    }

    private static StandardJanusGraph getGraph(String janus_config_file, Collection<Args.Option<String>> options){

        StandardJanusGraph graph;
        if (janus_config_file == null || janus_config_file.length() == 0){

            Map<String,String> configs = new HashMap<>(options.size());
            for (Args.Option<String> option:options){
                String key = option.metadata();
                String value = option.value();
                configs.put(key, value);

            }
            graph = GraphUtil.getGraph(configs);
        }else {
            graph = GraphUtil.getGraph(janus_config_file);
        }
        return graph;
    }


    public static Collection<Args.Option<File[]>> extractInputFiles(Args args, String key, PrintStream err )
    {
        return args
                .interpretOptionsWithMetadata( key, Converters.optional(),
                        Converters.toFiles( MULTI_FILE_DELIMITER, Converters.regexFiles( true ) ), filesExist(
                                err ),
                        Validators.atLeast( "--" + key, 1 ) );
    }

    public static Collection<Args.Option<String>> extractJanusConfig(Args args, String key, PrintStream err )
    {
        return args
                .interpretOptionsWithMetadata( key,
                        Converters.optional(),
                        Converters.identity());
    }

    /**
     * 得到输入数据
     * @param encoding
     * @param nodesFiles
     * @return
     */
    public static Iterable<DataFactory> nodeData(final Charset encoding,
                                                 Collection<Args.Option<File[]>> nodesFiles )
    {
        return new IterableWrapper<DataFactory,Args.Option<File[]>>( nodesFiles )
        {
            @Override
            protected DataFactory underlyingObjectToObject( Args.Option<File[]> input )
            {
                Decorator decorator = input.metadata() != null
                        ? InputEntityDecorators.additiveLabels( input.metadata().split( ":" ) )
                        : InputEntityDecorators.NO_DECORATOR;
                return data( decorator, encoding, input.value() );
            }
        };
    }

    public static Iterable<DataFactory> edgeData(final Charset encoding,
                                                 Collection<Args.Option<File[]>> edgesFiles )
    {
        return new IterableWrapper<DataFactory,Args.Option<File[]>>( edgesFiles )
        {
            @Override
            protected DataFactory underlyingObjectToObject( Args.Option<File[]> group )
            {
                return data( InputEntityDecorators.defaultEdgeLabel( group.metadata() ), encoding, group.value() );
            }
        };
    }

    /**
     * Creates a {@link DataFactory} where data exists in multiple files. If the first line of the first file is a header,
     * {@link # defaultFormatNodeFileHeader()} can be used to extract that.
     *
     * @param decorator Decorator for this data.
     * @param charset {@link Charset} to read data in.
     * @param files the files making up the data.
     *
     * @return {@link DataFactory} that returns a {@link CharSeeker} over all the supplied {@code files}.
     */
    public static DataFactory data( final Decorator decorator,
                                    final Charset charset, final File... files )
    {
        if ( files.length == 0 )
        {
            throw new IllegalArgumentException( "No files specified" );
        }

        return config -> new Data()
        {
            @Override
            public RawIterator<CharReadable,IOException> stream()
            {
                return Readables.individualFiles( charset, files );
            }

            @Override
            public Decorator decorator()
            {
                return decorator;
            }
        };
    }

    private static Validator<File[]> filesExist(PrintStream err )
    {
        return files ->
        {
            for ( File file : files )
            {
                if ( file.getName().startsWith( ":" ) )
                {
                    err.println( "It looks like you're trying to specify default label or EdgeLabel (" +
                            file.getName() + "). Please put such directly on the key, f.ex. " +
                            Options.NODE_DATA.argument() + ":MyLabel" );
                }
                Validators.REGEX_FILE_EXISTS.validate( file );
            }
        };
    }



    private static final String INPUT_FILES_DESCRIPTION =
            "Multiple files will be logically seen as one big file " +
                    "from the perspective of the importer. " +
                    "The first line must contain the header. " +
                    "Multiple data sources like these can be specified in one import, " +
                    "where each data source has its own header. " +
                    "Note that file groups must be enclosed in quotation marks. " +
                    "Each file can be a regular expression and will then include all matching files. " +
                    "The file matching is done with number awareness such that e.g. files:" +
                    "'File1Part_001.csv', 'File12Part_003' will be ordered in that order for a pattern like: 'File.*'";

    private static final String UNLIMITED = "true";

    private static final String CONF = "import.config";
    private static final int DEFAULT_MAX_MEMORY_PERCENT = 90;

    enum Options
    {
        FILE( "f", null,
                "<file name>",
                "File containing all arguments, used as an alternative to supplying all arguments on the command line directly."
                        + "Each argument can be on a separate line or multiple arguments per line separated by space."
                        + "Arguments containing spaces needs to be quoted."
                        + "Supplying other arguments in addition to this file argument is not supported." ),
        JANUS_CONFIG_FILE("janus-config-file",null,"<janusgraph configuration file name>",
                        "used as an alternative to supplying all janusgraph related arguments on the command line directly," +
                        "janusgraph configuration file contains all config , " +
                        "for example, storage.backend, storage.cassandra.keyspace, "),

        JANUS_CONFIG("janus-config",null,"<janusgraph config>","config to create janusgraph, which can be written into "
                + JANUS_CONFIG_FILE.key()),

        RECREATE_DATABASE_IF_EXISTS("drop-keyspace-if-exists",false,"<drop if exists>","bulk should only be used when init," +
                "so can we drop keyspace if exists ,or you should drop it by yourself if there already some data"),

        STORE_DIR( "into", null,
                "<store-dir>",
                "Database directory to import into. " + "Must not contain existing database." ),
        DB_NAME( "database", null,
                "<database-name>",
                "Database name to import into. " + "Must not contain existing database.", true ),
        NODE_DATA( "nodes", null,
                "[:Label1:Label2] \"<file1>" + MULTI_FILE_DELIMITER + "<file2>" + MULTI_FILE_DELIMITER + "...\"",
                "Node CSV header and data. " + INPUT_FILES_DESCRIPTION,
                true, true ),
        EDGE_DATA( "edges", null,
                "[:EDGE_LABEL] \"<file1>" + MULTI_FILE_DELIMITER + "<file2>" +
                        MULTI_FILE_DELIMITER + "...\"",
                "Edges CSV header and data. " + INPUT_FILES_DESCRIPTION,
                true, true ),
        DELIMITER( "delimiter", null,
                "<delimiter-character>",
                "Delimiter character, or 'TAB', between values in CSV data. The default option is `" + janusgraph.util.batchimport.unsafe.input.csv.Configuration.COMMAS.delimiter() + "`." ),
        ARRAY_DELIMITER( "array-delimiter", null,
                "<array-delimiter-character>",
                "Delimiter character, or 'TAB', between array elements within a value in CSV data. " +
                        "The default option is `" + janusgraph.util.batchimport.unsafe.input.csv.Configuration.COMMAS.arrayDelimiter() + "`." ),
        QUOTE( "quote", null,
                "<quotation-character>",
                "Character to treat as quotation character for values in CSV data. "
                        + "The default option is `" + janusgraph.util.batchimport.unsafe.input.csv.Configuration.COMMAS.quotationCharacter() + "`. "
                        + "Quotes inside quotes escaped like `\"\"\"Go away\"\", he said.\"` and "
                        + "`\"\\\"Go away\\\", he said.\"` are supported. "
                        + "If you have set \"`'`\" to be used as the quotation character, "
                        + "you could write the previous example like this instead: " + "`'\"Go away\", he said.'`" ),
        MULTILINE_FIELDS( "multiline-fields", janusgraph.util.batchimport.unsafe.input.reader.Configuration.DEFAULT.multilineFields(),
                "<true/false>",
                "Whether or not fields from input source can span multiple lines, i.e. contain newline characters." ),

        TRIM_STRINGS( "trim-strings", janusgraph.util.batchimport.unsafe.input.reader.Configuration.DEFAULT.trimStrings(),
                "<true/false>",
                "Whether or not strings should be trimmed for whitespaces." ),

        INPUT_ENCODING( "input-encoding", null,
                "<character set>",
                "Character set that input data is encoded in. Provided value must be one out of the available "
                        + "character sets in the JVM, as provided by Charset#availableCharsets(). "
                        + "If no input encoding is provided, the default character set of the JVM will be used.",
                true ),
        IGNORE_EMPTY_STRINGS( "ignore-empty-strings", janusgraph.util.batchimport.unsafe.input.reader.Configuration.DEFAULT.emptyQuotedStringsAsNull(),
                "<true/false>",
                "Whether or not empty string fields, i.e. \"\" from input source are ignored, i.e. treated as null." ),
        ID_TYPE( "id-type", IdType.STRING,
                "<id-type>",
                "One out of " + Arrays.toString( IdType.values() )
                        + " and specifies how ids in node/edge "
                        + "input files are treated.\n"
                        + IdType.STRING + ": arbitrary strings for identifying nodes.\n"
                        + IdType.INTEGER + ": arbitrary integer values for identifying nodes.\n"
                        + IdType.ACTUAL + ": (advanced) actual node ids. The default option is `" + IdType.STRING  +
                        "`.", true ),
        PROCESSORS( "processors", null,
                "<max processor count>",
                "(advanced) Max number of processors used by the importer. Defaults to the number of "
                        + "available processors reported by the JVM"
                        + availableProcessorsHint()
                        + ". There is a certain amount of minimum threads needed so for that reason there "
                        + "is no lower bound for this value. For optimal performance this value shouldn't be "
                        + "greater than the number of available processors." ),
        STACKTRACE( "stacktrace", false,
                "<true/false>",
                "Enable printing of error stack traces." ),
        BAD_TOLERANCE( "bad-tolerance", 1000,
                "<max number of bad entries, or " + UNLIMITED + " for unlimited>",
                "Number of bad entries before the import is considered failed. This tolerance threshold is "
                        + "about edges refering to missing nodes. Format errors in input data are "
                        + "still treated as errors" ),
        SKIP_BAD_ENTRIES_LOGGING( "skip-bad-entries-logging", Boolean.FALSE, "<true/false>",
                "Whether or not to skip logging bad entries detected during import." ),
        SKIP_BAD_EDGES( "skip-bad-edges", Boolean.TRUE,
                "<true/false>",
                "Whether or not to skip importing edges that refers to missing node ids, i.e. either "
                        + "start or end node id/group referring to node that wasn't specified by the "
                        + "node input data. "
                        + "Skipped nodes will be logged"
                        + ", containing at most number of entites specified by " + BAD_TOLERANCE.key() + ", unless "
                        + "otherwise specified by " + SKIP_BAD_ENTRIES_LOGGING.key() + " option." ),
        SKIP_DUPLICATE_NODES( "skip-duplicate-nodes", Boolean.FALSE,
                "<true/false>",
                "Whether or not to skip importing nodes that have the same id/group. In the event of multiple "
                        + "nodes within the same group having the same id, the first encountered will be imported "
                        + "whereas consecutive such nodes will be skipped. "
                        + "Skipped nodes will be logged"
                        + ", containing at most number of entities specified by " + BAD_TOLERANCE.key() + ", unless "
                        + "otherwise specified by " + SKIP_BAD_ENTRIES_LOGGING.key() + "option." ),
        IGNORE_EXTRA_COLUMNS( "ignore-extra-columns", Boolean.FALSE,
                "<true/false>",
                "Whether or not to ignore extra columns in the data not specified by the header. "
                        + "Skipped columns will be logged, containing at most number of entities specified by "
                        + BAD_TOLERANCE.key() + ", unless "
                        + "otherwise specified by " + SKIP_BAD_ENTRIES_LOGGING.key() + "option." ),
        DATABASE_CONFIG( "db-config", null, "<path/to/" + CONF + ">",
                "(advanced) Option is deprecated and replaced by 'additional-config'. " ),
        /*ADDITIONAL_CONFIG( "additional-config", null,
                "<path/to/" + CONF + ">",
                "(advanced) File specifying database-specific configuration. For more information consult "
                        + "manual about available configuration options for a janus configuration file. "
                        + "Only configuration affecting store at time of creation will be read. "
                        + "Examples of supported config are:\n"
                        *//*+ GraphDatabaseSettings.dense_node_threshold.name() + "\n"
                        + GraphDatabaseSettings.string_block_size.name() + "\n"
                        + GraphDatabaseSettings.array_block_size.name()*//*, true ),*/
        LEGACY_STYLE_QUOTING( "legacy-style-quoting", janusgraph.util.batchimport.unsafe.input.reader.Configuration.DEFAULT_LEGACY_STYLE_QUOTING,
                "<true/false>",
                "Whether or not backslash-escaped quote e.g. \\\" is interpreted as inner quote." ),
        READ_BUFFER_SIZE( "read-buffer-size", janusgraph.util.batchimport.unsafe.input.reader.Configuration.DEFAULT.bufferSize(),
                "<bytes, e.g. 10k, 4M>",
                "Size of each buffer for reading input data. It has to at least be large enough to hold the " +
                        "biggest single value in the input data." ),
        MAX_MEMORY( "max-memory", null,
                "<max memory that importer can use>",
                "(advanced) Maximum memory that importer can use for various data structures and caching " +
                        "to improve performance. If left as unspecified (null) it is set to " + DEFAULT_MAX_MEMORY_PERCENT +
                        "% of (free memory on machine - max JVM memory). " +
                        "Values can be plain numbers, like 10000000 or e.g. 20G for 20 gigabyte, or even e.g. 70%." ),
        CACHE_ON_HEAP( "cache-on-heap",
                false,
                "Whether or not to allow allocating memory for the cache on heap",
                "(advanced) Whether or not to allow allocating memory for the cache on heap. " +
                        "If 'false' then caches will still be allocated off-heap, but the additional free memory " +
                        "inside the JVM will not be allocated for the caches. This to be able to have better control " +
                        "over the heap memory" ),
        HIGH_IO( "high-io", null, "Assume a high-throughput storage subsystem",
                "(advanced) Ignore environment-based heuristics, and assume that the target storage subsystem can " +
                        "support parallel IO with high throughput." );

        private final String key;
        private final Object defaultValue;
        private final String usage;
        private final String description;
        private final boolean keyAndUsageGoTogether;
        private final boolean supported;

        Options( String key, Object defaultValue, String usage, String description )
        {
            this( key, defaultValue, usage, description, false, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported )
        {
            this( key, defaultValue, usage, description, supported, false );
        }

        Options( String key, Object defaultValue, String usage, String description, boolean supported, boolean keyAndUsageGoTogether )
        {
            this.key = key;
            this.defaultValue = defaultValue;
            this.usage = usage;
            this.description = description;
            this.supported = supported;
            this.keyAndUsageGoTogether = keyAndUsageGoTogether;
        }

        String key()
        {
            return key;
        }

        String argument()
        {
            return "--" + key();
        }

        void printUsage( PrintStream out )
        {
            out.println( argument() + spaceInBetweenArgumentAndUsage() + usage );
            for ( String line : Args.splitLongLine( descriptionWithDefaultValue().replace( "`", "" ), 80 ) )
            {
                out.println( "\t" + line );
            }
        }

        private String spaceInBetweenArgumentAndUsage()
        {
            return keyAndUsageGoTogether ? "" : " ";
        }

        String descriptionWithDefaultValue()
        {
            String result = description;
            if ( defaultValue != null )
            {
                if ( !result.endsWith( "." ) )
                {
                    result += ".";
                }
                result += " Default value: " + defaultValue;
            }
            return result;
        }

        String manPageEntry()
        {
            String filteredDescription = descriptionWithDefaultValue().replace( availableProcessorsHint(), "" );
            String usageString = (usage.length() > 0) ? spaceInBetweenArgumentAndUsage() + usage : "";
            return "*" + argument() + usageString + "*::\n" + filteredDescription + "\n\n";
        }

        String manualEntry()
        {
            return "[[import-tool-option-" + key() + "]]\n" + manPageEntry() + "//^\n\n";
        }

        Object defaultValue()
        {
            return defaultValue;
        }

        private static String availableProcessorsHint()
        {
            return " (in your case " + Runtime.getRuntime().availableProcessors() + ")";
        }

        public boolean isSupportedOption()
        {
            return this.supported;
        }
    }

    private static Collector getBadCollector( long badTolerance, boolean skipBadEdges,
                                              boolean skipDuplicateNodes, boolean ignoreExtraColumns, boolean skipBadEntriesLogging,
                                              OutputStream badOutput )
    {
        int collect = Collectors.collect( skipBadEdges, skipDuplicateNodes, ignoreExtraColumns );
        return skipBadEntriesLogging ? silentBadCollector( badTolerance, collect ) : badCollector( badOutput, badTolerance, collect );
    }

    public static janusgraph.util.batchimport.unsafe.input.csv.Configuration csvConfiguration(
            Args args, final boolean defaultSettingsSuitableForTests )
    {
        final janusgraph.util.batchimport.unsafe.input.csv.Configuration defaultConfiguration = janusgraph.util.batchimport.unsafe.input.csv.Configuration.COMMAS;
        final Character specificDelimiter = args.interpretOption( Options.DELIMITER.key(),
                Converters.optional(), CHARACTER_CONVERTER );
        final Character specificArrayDelimiter = args.interpretOption( Options.ARRAY_DELIMITER.key(),
                Converters.optional(), CHARACTER_CONVERTER );
        final Character specificQuote = args.interpretOption( Options.QUOTE.key(), Converters.optional(),
                CHARACTER_CONVERTER );
        final Boolean multiLineFields = args.getBoolean( Options.MULTILINE_FIELDS.key(), null );
        final Boolean emptyStringsAsNull = args.getBoolean( Options.IGNORE_EMPTY_STRINGS.key(), null );
        final Boolean trimStrings = args.getBoolean( Options.TRIM_STRINGS.key(), null);
        final Boolean legacyStyleQuoting = args.getBoolean( Options.LEGACY_STYLE_QUOTING.key(), null );
        final Number bufferSize = args.has( Options.READ_BUFFER_SIZE.key() )
                ? parseLongWithUnit( args.get( Options.READ_BUFFER_SIZE.key(), null ) )
                : null;
        return new janusgraph.util.batchimport.unsafe.input.csv.Configuration.Default()
        {
            @Override
            public char delimiter()
            {
                return specificDelimiter != null
                        ? specificDelimiter.charValue()
                        : defaultConfiguration.delimiter();
            }

            @Override
            public char arrayDelimiter()
            {
                return specificArrayDelimiter != null
                        ? specificArrayDelimiter.charValue()
                        : defaultConfiguration.arrayDelimiter();
            }

            @Override
            public char quotationCharacter()
            {
                return specificQuote != null
                        ? specificQuote.charValue()
                        : defaultConfiguration.quotationCharacter();
            }

            @Override
            public boolean multilineFields()
            {
                return multiLineFields != null
                        ? multiLineFields.booleanValue()
                        : defaultConfiguration.multilineFields();
            }

            @Override
            public boolean emptyQuotedStringsAsNull()
            {
                return emptyStringsAsNull != null
                        ? emptyStringsAsNull.booleanValue()
                        : defaultConfiguration.emptyQuotedStringsAsNull();
            }

            @Override
            public int bufferSize()
            {
                return bufferSize != null
                        ? bufferSize.intValue()
                        : defaultSettingsSuitableForTests ? 10_000 : super.bufferSize();
            }

            @Override
            public boolean trimStrings()
            {
                return trimStrings != null
                        ? trimStrings.booleanValue()
                        : defaultConfiguration.trimStrings();
            }

            @Override
            public boolean legacyStyleQuoting()
            {
                return legacyStyleQuoting != null
                        ? legacyStyleQuoting.booleanValue()
                        : defaultConfiguration.legacyStyleQuoting();
            }
        };
    }

    // Setting converters and constraints
    public static long parseLongWithUnit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = findFirstNonDigit( numberWithPotentialUnit );
        String number = numberWithPotentialUnit.substring( 0, firstNonDigitIndex );

        long multiplier = 1;
        if ( firstNonDigitIndex < numberWithPotentialUnit.length() )
        {
            String unit = numberWithPotentialUnit.substring( firstNonDigitIndex );
            if ( unit.equalsIgnoreCase( "k" ) )
            {
                multiplier = 1024;
            }
            else if ( unit.equalsIgnoreCase( "m" ) )
            {
                multiplier = 1024 * 1024;
            }
            else if ( unit.equalsIgnoreCase( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
            }
            else
            {
                throw new IllegalArgumentException(
                        "Illegal unit '" + unit + "' for number '" + numberWithPotentialUnit + "'" );
            }
        }

        return parseLong( number ) * multiplier;
    }

    /**
     * @return index of first non-digit character in {@code numberWithPotentialUnit}. If all digits then
     * {@code numberWithPotentialUnit.length()} is returned.
     */
    private static int findFirstNonDigit( String numberWithPotentialUnit )
    {
        int firstNonDigitIndex = numberWithPotentialUnit.length();
        for ( int i = 0; i < numberWithPotentialUnit.length(); i++ )
        {
            if ( !isDigit( numberWithPotentialUnit.charAt( i ) ) )
            {
                firstNonDigitIndex = i;
                break;
            }
        }
        return firstNonDigitIndex;
    }


    private static CharSeeker seeker( String definition, janusgraph.util.batchimport.unsafe.input.csv.Configuration config )
    {
        return CharSeekers.charSeeker( Readables.wrap( definition ),
                new janusgraph.util.batchimport.unsafe.input.csv.Configuration.Overridden( config )
                {
                    @Override
                    public int bufferSize()
                    {
                        return 10_000;
                    }
                }, false );
    }

    private static boolean asksForUsage( Args args )
    {
        for ( String orphan : args.orphans() )
        {
            if ( isHelpKey( orphan ) )
            {
                return true;
            }
        }

        for ( Map.Entry<String,String> option : args.asMap().entrySet() )
        {
            if ( isHelpKey( option.getKey() ) )
            {
                return true;
            }
        }
        return false;
    }

    private static boolean isHelpKey( String key )
    {
        return key.equals( "?" ) || key.equals( "help" );
    }

    private static void printUsage( PrintStream out )
    {
        out.println( "Janus Import Tool" );
        for ( String line : Args.splitLongLine( "janusgraph-import is used to create a new janusgraph database "
                + "from data in CSV files. "
                +
                "See the chapter \"Bulk Load Tool\" in the JanusGraph Manual for details on the CSV file format "
                + "- a special kind of header is required.", 80 ) )
        {
            out.println( "\t" + line );
        }
        out.println( "Usage:" );
        for ( Options option : Options.values() )
        {
            option.printUsage( out );
        }

        out.println( "Example:");
        out.print( Strings.joinAsLines(
                Strings.TAB + "bin/janusgraph-import --into retail.db --id-type string --nodes:Customer customers.csv ",
                Strings.TAB + "--nodes products.csv --nodes orders_header.csv,orders1.csv,orders2.csv ",
                Strings.TAB + "--edges:CONTAINS order_details.csv ",
                Strings.TAB + "--edges:ORDERED customer_orders_header.csv,orders1.csv,orders2.csv" ) );
    }


    public static Args useArgumentsFromFileArgumentIfPresent( Args args ) throws IOException
    {
        String fileArgument = args.get( Options.FILE.key(), null );
        if ( fileArgument != null )
        {
            // Are there any other arguments supplied, in addition to this -f argument?
            if ( args.asMap().size() > 1 )
            {
                throw new IllegalArgumentException(
                        "Supplying arguments in addition to " + Options.FILE.argument() + " isn't supported." );
            }

            // Read the arguments from the -f file and use those instead
            args = Args.parse( parseFileArgumentList( new File( fileArgument ) ) );
        }
        return args;
    }

    public static String[] parseFileArgumentList( File file ) throws IOException
    {
        List<String> arguments = new ArrayList<>();
        FileUtils.readTextFile( file, line -> arguments.addAll( asList( tokenizeStringWithQuotes( line, true, true ) ) ) );
        return arguments.toArray( new String[arguments.size()] );
    }

    static Long parseMaxMemory( String maxMemoryString )
    {
        if ( maxMemoryString != null )
        {
            maxMemoryString = maxMemoryString.trim();
            if ( maxMemoryString.endsWith( "%" ) )
            {
                int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
                long result = Configuration.calculateMaxMemoryFromPercent( percent );
                if ( !canDetectFreeMemory() )
                {
                    System.err.println( "WARNING: amount of free memory couldn't be detected so defaults to " +
                            ByteUnit.bytes( result ) + ". For optimal performance instead explicitly specify amount of " +
                            "memory that importer is allowed to use using " + Options.MAX_MEMORY.argument() );
                }
                return result;
            }
        }
        return null;
    }

    static boolean canDetectFreeMemory()
    {
        return OsBeanUtil.getFreePhysicalMemory() != OsBeanUtil.VALUE_UNAVAILABLE;
    }


    public static void validateInputFiles( Collection<Args.Option<File[]>> nodesFiles,
                                           Collection<Args.Option<File[]>> edgesFiles )
    {
        if ( nodesFiles.isEmpty() )
        {
            if ( edgesFiles.isEmpty() )
            {
                throw new IllegalArgumentException( "No input specified, nothing to import" );
            }
            throw new IllegalArgumentException( "No node input specified, cannot import edges without nodes" );
        }
    }

    private static long parseNumberOrUnlimited( Args args, Options option )
    {
        String value = args.get( option.key(), option.defaultValue().toString() );
        return UNLIMITED.equals( value ) ? BadCollector.UNLIMITED_TOLERANCE : Long.parseLong( value );
    }

    public static Configuration importConfiguration(
            Number processors, Long maxMemory, File storeDir,
            boolean allowCacheOnHeap, Boolean defaultHighIO )
    {
        return new Configuration()
        {
            @Override
            public long pageCacheMemory()
            {
                return ByteUnit.mebiBytes( 8 ) ;
            }

            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors.intValue() : 1;
            }


            @Override
            public long maxMemoryUsage()
            {
                return maxMemory != null ? maxMemory : 4294967296L;
            }

            @Override
            public boolean parallelRecordReadsWhenWriting()
            {
                return defaultHighIO != null ? defaultHighIO : FileUtils.highIODevice( storeDir.toPath(), false );
            }

            @Override
            public boolean allowCacheAllocationOnHeap()
            {
                return allowCacheOnHeap;
            }
        };
    }

    private static RuntimeException andPrintError( String typeOfError, Exception e, boolean stackTrace,
                                                   PrintStream err )
    {
        // List of common errors that can be explained to the user
        if ( DuplicateInputIdException.class.equals( e.getClass() ) )
        {
            printErrorMessage( "Duplicate input ids that would otherwise clash can be put into separate id space, " +
                            "read more about how to use id spaces in the manual:" +
                            manualReference( ), e, stackTrace,
                    err );
        }
        else if ( MissingEdgeDataException.class.equals( e.getClass() ) )
        {
            printErrorMessage( "Edge missing mandatory field '" +
                            ((MissingEdgeDataException) e).getFieldType() + "', read more about " +
                            "edge format in the manual: " +
                            manualReference(  ), e, stackTrace,
                    err );
        }
        // This type of exception is wrapped since our input code throws InputException consistently,
        // and so IllegalMultilineFieldException comes from the csv component, which has no access to InputException
        // therefore it's wrapped.
        else if ( Exceptions.contains( e, IllegalMultilineFieldException.class ) )
        {
            printErrorMessage( "Detected field which spanned multiple lines for an import where " +
                    Options.MULTILINE_FIELDS.argument() + "=false. If you know that your input data " +
                    "include fields containing new-line characters then import with this option set to " +
                    "true.", e, stackTrace, err );
        }
        else if ( Exceptions.contains( e, InputException.class ) )
        {
            printErrorMessage( "Error in input data", e, stackTrace, err );
        }
        // Fallback to printing generic error and stack trace
        else
        {
            printErrorMessage( typeOfError + ": " + e.getMessage(), e, true, err );
        }
        err.println();

        // Mute the stack trace that the default exception handler would have liked to print.
        // Calling System.exit( 1 ) or similar would be convenient on one hand since we can set
        // a specific exit code. On the other hand It's very inconvenient to have any System.exit
        // call in code that is tested.
        Thread.currentThread().setUncaughtExceptionHandler( ( t, e1 ) ->
        {
            /* Shhhh */
        } );
        return launderedException( e ); // throw in order to have process exit with !0
    }

    private static void printOverview(File storeDir, Collection<Args.Option<File[]>> nodesFiles,
                                      Collection<Args.Option<File[]>> edgesFiles,
                                      Configuration configuration, PrintStream out)
    {
        out.println( "Importing the contents of these files into " + storeDir + ":" );
        printInputFiles( "Nodes", nodesFiles, out );
        printInputFiles( "Edges", edgesFiles, out );
        out.println();
        out.println( "Available resources:" );
        printIndented( "Total machine memory: " + ByteUnit.bytes( OsBeanUtil.getTotalPhysicalMemory() ), out );
        printIndented( "Free machine memory: " + ByteUnit.bytes( OsBeanUtil.getFreePhysicalMemory() ), out );
        printIndented( "Max heap memory : " + ByteUnit.bytes( Runtime.getRuntime().maxMemory() ), out );
        printIndented( "Processors: " + configuration.maxNumberOfProcessors(), out );
        printIndented( "Configured max memory: " + ByteUnit.bytes( configuration.maxMemoryUsage() ), out );
        out.println();
    }

    private static void printInputFiles( String name, Collection<Args.Option<File[]>> files, PrintStream out )
    {
        if ( files.isEmpty() )
        {
            return;
        }

        out.println( name + ":" );
        int i = 0;
        for ( Args.Option<File[]> group : files )
        {
            if ( i++ > 0 )
            {
                out.println();
            }
            if ( group.metadata() != null )
            {
                printIndented( ":" + group.metadata(), out );
            }
            for ( File file : group.value() )
            {
                printIndented( file, out );
            }
        }
    }

    private static void printIndented( Object value, PrintStream out )
    {
        out.println( "  " + value );
    }

    private static String manualReference( )
    {
        // Docs are versioned major.minor-suffix, so drop the patch version.


        return "mail to dengziming@u51.com to fix";
    }

    private static void printErrorMessage( String string, Exception e, boolean stackTrace, PrintStream err )
    {
        err.println( string );
        err.println( "Caused by:" + e.getMessage() );
        if ( stackTrace )
        {
            e.printStackTrace( err );
        }
    }
}
