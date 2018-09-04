package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.OsBeanUtil;
import janusgraph.util.batchimport.unsafe.helps.DependencyResolver;
import janusgraph.util.batchimport.unsafe.helps.Format;
import janusgraph.util.batchimport.unsafe.helps.Pair;
import janusgraph.util.batchimport.unsafe.io.MeasureDoNothing;
import janusgraph.util.batchimport.unsafe.stats.DetailLevel;
import janusgraph.util.batchimport.unsafe.stats.Keys;
import janusgraph.util.batchimport.unsafe.stats.Stat;
import janusgraph.util.batchimport.unsafe.stats.StepStats;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;

import static java.lang.Long.max;
import static java.lang.System.currentTimeMillis;

/**
 * Sits in the background and collect stats about stages that are executing.
 * Reacts to console input from {@link System#in} and on certain commands print various things.
 * Commands (complete all with ENTER):
 * <p>
 * <table border="1">
 *   <tr>
 *     <th>Command</th>
 *     <th>Description</th>
 *   </tr>
 *   <tr>
 *     <th>i</th>
 *     <th>Print {@link SpectrumExecutionMonitor compact information} about each executed stage up to this point</th>
 *   </tr>
 * </table>
 */
public class OnDemandDetailsExecutionMonitor implements ExecutionMonitor
{
    interface Monitor
    {
        void detailsPrinted();
    }

    private final List<StageDetails> details = new ArrayList<>();
    private final PrintStream out;
    private final InputStream in;
    private final Map<String,Pair<String,Runnable>> actions = new HashMap<>();
    private final MeasureDoNothing.CollectingMonitor gcBlockTime = new MeasureDoNothing.CollectingMonitor();
    private final MeasureDoNothing gcMonitor;
    private final Monitor monitor;

    private StageDetails current;
    private boolean printDetailsOnDone;

    public OnDemandDetailsExecutionMonitor(PrintStream out, InputStream in, Monitor monitor )
    {
        this.out = out;
        this.in = in;
        this.monitor = monitor;
        this.actions.put( "i", Pair.of( "Print more detailed information", this::printDetails ) );
        this.actions.put( "c", Pair.of( "Print more detailed information about current stage", this::printDetailsForCurrentStage ) );
        this.gcMonitor = new MeasureDoNothing( "Importer GC monitor", gcBlockTime, 100, 200 );
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        out.println( "InteractiveReporterInteractions command list (end with ENTER):" );
        actions.forEach( ( key, action ) -> out.println( "  " + key + ": " + action.first() ) );
        out.println();
        gcMonitor.start();
    }

    @Override
    public void start( StageExecution execution )
    {
        details.add( current = new StageDetails( execution, gcBlockTime ) );
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        current.collect();
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        if ( printDetailsOnDone )
        {
            printDetails();
        }
        gcMonitor.stopMeasuring();
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 500;
    }

    @Override
    public void check( StageExecution execution )
    {
        current.collect();
        reactToUserInput();
    }

    private void printDetails()
    {
        printDetailsHeadline();
        long totalTime = 0;
        for ( StageDetails stageDetails : details )
        {
            stageDetails.print( out );
            totalTime += stageDetails.totalTimeMillis;
        }

        printIndented( out, "Environment information:" );
        printIndented( out, "  Free physical memory: " + Format.bytes( OsBeanUtil.getFreePhysicalMemory() ) );
        printIndented( out, "  Max VM memory: " + Format.bytes( Runtime.getRuntime().maxMemory() ) );
        printIndented( out, "  Free VM memory: " + Format.bytes( Runtime.getRuntime().freeMemory() ) );
        printIndented( out, "  GC block time: " + Format.duration( gcBlockTime.getGcBlockTime() ) );
        printIndented( out, "  Duration: " + Format.duration( totalTime ) );
        out.println();
    }

    private void printDetailsHeadline()
    {
        out.println();
        out.println();
        printIndented( out, "******** DETAILS " + Format.date() + " ********" );
        out.println();

        // Make sure that if user is interested in details then also print the entire details set when import is done
        printDetailsOnDone = true;
        monitor.detailsPrinted();
    }

    private void printDetailsForCurrentStage()
    {
        printDetailsHeadline();
        if ( !details.isEmpty() )
        {
            details.get( details.size() - 1 ).print( out );
        }
    }

    private static void printIndented( PrintStream out, String string )
    {
        out.println( "\t" + string );
    }

    private void reactToUserInput()
    {
        try
        {
            if ( in.available() > 0 )
            {
                // don't close this read, since we really don't want to close the underlying System.in
                BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) );
                String line = reader.readLine();
                Pair<String,Runnable> action = actions.get( line );
                if ( action != null )
                {
                    action.other().run();
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace( out );
        }
    }

    private static class StageDetails
    {
        private final StageExecution execution;
        private final long startTime;
        private final MeasureDoNothing.CollectingMonitor gcBlockTime;
        private final long baseGcBlockTime;

        private long memoryUsage;
        private long ioThroughput;
        private long totalTimeMillis;
        private long stageGcBlockTime;
        private long doneBatches;

        StageDetails( StageExecution execution, MeasureDoNothing.CollectingMonitor gcBlockTime )
        {
            this.execution = execution;
            this.gcBlockTime = gcBlockTime;
            this.startTime = currentTimeMillis();
            this.baseGcBlockTime = gcBlockTime.getGcBlockTime();
        }

        void print( PrintStream out )
        {
            printIndented( out, execution.name() );
            StringBuilder builder = new StringBuilder();
            SpectrumExecutionMonitor.printSpectrum( builder, execution, SpectrumExecutionMonitor.DEFAULT_WIDTH, DetailLevel.NO );
            printIndented( out, builder.toString() );
            printValue( out, memoryUsage, "Memory usage", Format::bytes );
            printValue( out, ioThroughput, "I/O throughput", value -> Format.bytes( value ) + "/s" );
            printValue( out, stageGcBlockTime, "GC block time", Format::duration );
            printValue( out, totalTimeMillis, "Duration", Format::duration );
            printValue( out, doneBatches, "Done batches", String::valueOf );

            out.println();
        }

        private static void printValue( PrintStream out, long value, String description, LongFunction<String> toStringConverter )
        {
            if ( value > 0 )
            {
                printIndented( out, description + ": " + toStringConverter.apply( value ) );
            }
        }

        void collect()
        {
            totalTimeMillis = currentTimeMillis() - startTime;
            stageGcBlockTime = gcBlockTime.getGcBlockTime() - baseGcBlockTime;
            long lastDoneBatches = doneBatches;
            for ( Step<?> step : execution.steps() )
            {
                StepStats stats = step.stats();
                Stat memoryUsageStat = stats.stat( Keys.memory_usage );
                if ( memoryUsageStat != null )
                {
                    memoryUsage = max( memoryUsage, memoryUsageStat.asLong() );
                }
                Stat ioStat = stats.stat( Keys.io_throughput );
                if ( ioStat != null )
                {
                    ioThroughput = ioStat.asLong();
                }
                lastDoneBatches = stats.stat( Keys.done_batches ).asLong();
            }
            doneBatches = lastDoneBatches;
        }
    }
}
