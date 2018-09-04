package janusgraph.util.batchimport.unsafe;

import java.io.PrintStream;

class PrintingImportLogicMonitor implements ImportLogic.Monitor
{
    private final PrintStream out;
    private final PrintStream err;

    PrintingImportLogicMonitor(PrintStream out, PrintStream err )
    {
        this.out = out;
        this.err = err;
    }

    @Override
    public void doubleRelationshipRecordUnitsEnabled()
    {
        out.println( "Will use double record units for all relationships" );
    }

    @Override
    public void mayExceedNodeIdCapacity( long capacity, long estimatedCount )
    {
        err.printf( "WARNING: estimated number of relationships %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity );
    }

    @Override
    public void mayExceedRelationshipIdCapacity( long capacity, long estimatedCount )
    {
        err.printf( "WARNING: estimated number of nodes %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity );
    }
}
