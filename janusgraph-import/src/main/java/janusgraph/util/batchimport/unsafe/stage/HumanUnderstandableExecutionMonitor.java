package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.DataImporter;
import janusgraph.util.batchimport.unsafe.helps.DependencyResolver;
import janusgraph.util.batchimport.unsafe.helps.collection.Iterables;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.input.csv.Input;
import janusgraph.util.batchimport.unsafe.stats.DataStatistics;
import janusgraph.util.batchimport.unsafe.stats.Keys;
import janusgraph.util.batchimport.unsafe.stats.Stat;
import janusgraph.util.batchimport.unsafe.helps.ByteUnit;
import janusgraph.util.batchimport.unsafe.helps.Format;

import java.io.PrintStream;
import java.util.TimeZone;

import static java.lang.Integer.min;
import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

/**
 * Prints progress you can actually understand, with capabilities to on demand print completely incomprehensible
 * details only understandable to a select few.
 */
public class HumanUnderstandableExecutionMonitor implements ExecutionMonitor
{
    public interface Monitor
    {
        void progress(ImportStage stage, int percent);
    }

    public static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void progress( ImportStage stage, int percent )
        {   // empty
        }
    };

    public interface ExternalMonitor
    {
        boolean somethingElseBrokeMyNiceOutput();
    }

    public static final ExternalMonitor NO_EXTERNAL_MONITOR = new ExternalMonitor()
    {
        @Override
        public boolean somethingElseBrokeMyNiceOutput()
        {
            return false;
        }
    };

    enum ImportStage
    {
        nodeImport,
        edgeImport,
        linking,
        postProcessing;
    }

    private static final String ESTIMATED_REQUIRED_MEMORY_USAGE = "Estimated required memory usage";
    private static final String ESTIMATED_DISK_SPACE_USAGE = "Estimated disk space usage";
    private static final String ESTIMATED_NUMBER_OF_EDGE_PROPERTIES = "Estimated number of edge properties";
    private static final String ESTIMATED_NUMBER_OF_EDGES = "Estimated number of edge";
    private static final String ESTIMATED_NUMBER_OF_NODE_PROPERTIES = "Estimated number of node properties";
    private static final String ESTIMATED_NUMBER_OF_NODES = "Estimated number of nodes";
    private static final int DOT_GROUP_SIZE = 10;
    private static final int DOT_GROUPS_PER_LINE = 5;
    private static final int PERCENTAGES_PER_LINE = 5;

    // assigned later on
    private final PrintStream out;
    private final Monitor monitor;
    private final ExternalMonitor externalMonitor;
    private DependencyResolver dependencyResolver;

    // progress of current stage
    private long goal;
    private long stashedProgress;
    private long progress;
    private ImportStage currentStage;

    public HumanUnderstandableExecutionMonitor(PrintStream out, Monitor monitor, ExternalMonitor externalMonitor )
    {
        this.out = out;
        this.monitor = monitor;
        this.externalMonitor = externalMonitor;
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        this.dependencyResolver = dependencyResolver;
        Input.Estimates estimates = dependencyResolver.resolveDependency( Input.Estimates.class );
        IdMapper idMapper = dependencyResolver.resolveDependency( IdMapper.class );

        long biggestCacheMemory = defensivelyPadMemoryEstimate( max(
                idMapper.calculateMemoryUsage( estimates.numberOfNodes() ),
                ( estimates.numberOfNodes() ) ) );
        printStageHeader( "Import starting",
                ESTIMATED_NUMBER_OF_NODES, Format.count( estimates.numberOfNodes() ),
                ESTIMATED_NUMBER_OF_NODE_PROPERTIES, Format.count( estimates.numberOfNodeProperties() ),
                ESTIMATED_NUMBER_OF_EDGES, Format.count( estimates.numberOfEdges() ),
                ESTIMATED_NUMBER_OF_EDGE_PROPERTIES, Format.count( estimates.numberOfEdgeProperties() ),
                ESTIMATED_DISK_SPACE_USAGE, ByteUnit.bytes(

                                // TODO also add some padding to include edge groups?
                                estimates.sizeOfNodeProperties() + estimates.sizeOfEdgeProperties() ),
                ESTIMATED_REQUIRED_MEMORY_USAGE, ByteUnit.bytes( biggestCacheMemory ) );
        out.println();
    }

    @Override
    public void start( StageExecution execution )
    {
        // Divide into 4 progress stages:
        if ( execution.getStageName().equals( DataImporter.NODE_IMPORT_NAME ) )
        {
            // Import nodes:
            // - import nodes
            // - prepare id mapper
            initializeNodeImport(
                    dependencyResolver.resolveDependency( Input.Estimates.class ),
                    dependencyResolver.resolveDependency( IdMapper.class ));
        }
        else if ( execution.getStageName().equals( DataImporter.EDGE_IMPORT_NAME) )
        {

            endPrevious();
            // Import edges:
            // - import edges
            initializeEdgeImport(
                    dependencyResolver.resolveDependency( Input.Estimates.class ),
                    dependencyResolver.resolveDependency( IdMapper.class ));
        }
        else if ( includeStage( execution ) )
        {
            stashedProgress += progress;
            progress = 0;
        }
    }

    private void endPrevious()
    {
        updateProgress( goal ); // previous ended
        // TODO print some end stats for this stage?
    }

    private void initializeNodeImport(Input.Estimates estimates, IdMapper idMapper)
    {
        // TODO how to handle UNKNOWN?
        long numberOfNodes = estimates.numberOfNodes();
        printStageHeader( "(1/2) Node import",
                ESTIMATED_NUMBER_OF_NODES, Format.count( numberOfNodes ),
                ESTIMATED_DISK_SPACE_USAGE, ByteUnit.bytes(
                        // node store
                        0 +
                        // property store(s)
                        estimates.sizeOfNodeProperties() ),
                ESTIMATED_REQUIRED_MEMORY_USAGE, ByteUnit.bytes(
//                        baselineMemoryRequirement( janusStores ) +
                        defensivelyPadMemoryEstimate( idMapper.calculateMemoryUsage( numberOfNodes ) ) ) );

        // A difficulty with the goal here is that we don't know how much woek there is to be done in id mapper preparation stage.
        // In addition to nodes themselves and SPLIT,SORT,DETECT there may be RESOLVE,SORT,DEDUPLICATE too, if there are collisions
        long goal = idMapper.needsPreparation()
                ? (long) (numberOfNodes + weighted( IdMapperPreparationStage.NAME, numberOfNodes * 4 ))
                : numberOfNodes;
        initializeProgress( goal, ImportStage.nodeImport );
    }

    private void initializeEdgeImport(Input.Estimates estimates, IdMapper idMapper )
    {
        long numberOfEdges = estimates.numberOfEdges();
        printStageHeader( "(2/2) Edge import",
                ESTIMATED_NUMBER_OF_EDGES, Format.count( numberOfEdges ),
                ESTIMATED_DISK_SPACE_USAGE, ByteUnit.bytes(
                        /*edgessDiskUsage( estimates, janusStores ) +*/
                        estimates.sizeOfEdgeProperties() ),
                ESTIMATED_REQUIRED_MEMORY_USAGE, ByteUnit.bytes(
                        /*baselineMemoryRequirement( janusStores ) +*/
                        GatheringMemoryStatsVisitor.totalMemoryUsageOf( idMapper ) ) );
        initializeProgress( numberOfEdges, ImportStage.edgeImport);
    }


    private static long defensivelyPadMemoryEstimate( long bytes )
    {
        return (long) (bytes * 1.1);
    }

    private void initializeProgress( long goal, ImportStage stage )
    {
        this.goal = goal;
        this.stashedProgress = 0;
        this.progress = 0;
        this.currentStage = stage;
    }

    private void updateProgress( long progress )
    {
        // OK so format goes something like 5 groups of 10 dots per line, which is 5%, i.e. 50 dots for 5%, i.e. 1000 dots for 100%,
        // i.e. granularity is 1/1000

        int maxDot = dotOf( goal );
        int currentProgressDot = dotOf( stashedProgress + this.progress );
        int currentLine = currentProgressDot / dotsPerLine();
        int currentDotOnLine = currentProgressDot % dotsPerLine();

        int progressDot = min( maxDot, dotOf( stashedProgress + progress ) );
        int line = progressDot / dotsPerLine();
        int dotOnLine = progressDot % dotsPerLine();

        while ( currentLine < line || (currentLine == line && currentDotOnLine < dotOnLine) )
        {
            int target = currentLine < line ? dotsPerLine() : dotOnLine;
            printDots( currentDotOnLine, target );
            currentDotOnLine = target;

            if ( currentLine < line || currentDotOnLine == dotsPerLine() )
            {
                int percentage = percentage( currentLine );
                out.println( format( " %s%%", percentage ) );
                monitor.progress( currentStage, percentage );
                currentLine++;
                if ( currentLine == lines() )
                {
                    out.println();
                }
                currentDotOnLine = 0;
            }
        }

        // TODO not quite right
        this.progress = max( this.progress, progress );
    }

    private static int percentage( int line )
    {
        return (line + 1) * PERCENTAGES_PER_LINE;
    }

    private void printDots( int from, int target )
    {
        int current = from;
        while ( current < target )
        {
            if ( current > 0 && current % DOT_GROUP_SIZE == 0 )
            {
                out.print( " " );
            }
            out.print( "." );
            current++;
        }
    }

    private int dotOf( long progress )
    {
        // calculated here just to reduce amount of state kept in this instance
        int dots = dotsPerLine() * lines();
        double dotSize = goal / (double) dots;

        return (int) (progress / dotSize);
    }

    private static int lines()
    {
        return 100 / PERCENTAGES_PER_LINE;
    }

    private static int dotsPerLine()
    {
        return DOT_GROUPS_PER_LINE * DOT_GROUP_SIZE;
    }

    private void printStageHeader( String name, Object... data )
    {
        out.println( name + " " + Format.date( TimeZone.getDefault() ) );
        if ( data.length > 0 )
        {
            for ( int i = 0; i < data.length; )
            {
                out.println( "  " + data[i++] + ": " + data[i++] );
            }
        }
    }



    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        endPrevious();

        out.println();
        out.println( "IMPORT DONE in " + Format.duration( totalTimeMillis ) + ". " + additionalInformation );
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 200;
    }

    @Override
    public void check( StageExecution execution )
    {
        reprintProgressIfNecessary();
        if ( includeStage( execution ) )
        {
            updateProgress( progressOf( execution ) );
        }
    }

    private void reprintProgressIfNecessary()
    {
        if ( externalMonitor.somethingElseBrokeMyNiceOutput() )
        {
            long prevProgress = this.progress;
            long prevStashedProgress = this.stashedProgress;
            this.progress = 0;
            this.stashedProgress = 0;
            updateProgress( prevProgress + prevStashedProgress );
            this.progress = prevProgress;
            this.stashedProgress = prevStashedProgress;
        }
    }

    private static boolean includeStage( StageExecution execution )
    {
        String name = execution.getStageName();
        return true;
    }

    private static double weightOf( String stageName )
    {
        if ( stageName.equals( IdMapperPreparationStage.NAME ) )
        {
            return 0.5D;
        }
        return 1;
    }

    private static long weighted( String stageName, long progress )
    {
        return (long) (progress * weightOf( stageName ));
    }

    private static long progressOf( StageExecution execution )
    {
        // First see if there's a "progress" stat
        Stat progressStat = findProgressStat( execution.steps() );
        if ( progressStat != null )
        {
            return weighted( execution.getStageName(), progressStat.asLong() );
        }

        // No, then do the generic progress calculation by looking at "done_batches"
        long doneBatches = Iterables.last( execution.steps() ).stats().stat( Keys.done_batches ).asLong();
        int batchSize = execution.getConfig().batchSize();
        long progress = weighted( execution.getStageName(), doneBatches * batchSize );
        return progress;
    }

    private static Stat findProgressStat( Iterable<Step<?>> steps )
    {
        for ( Step<?> step : steps )
        {
            Stat stat = step.stats().stat( Keys.progress );
            if ( stat != null )
            {
                return stat;
            }
        }
        return null;
    }
}
