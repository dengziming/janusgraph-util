package janusgraph.util.batchimport.unsafe.input;


import janusgraph.util.batchimport.unsafe.helps.DuplicateInputIdException;
import janusgraph.util.batchimport.unsafe.helps.collection.concurrent.AsyncEvent;
import janusgraph.util.batchimport.unsafe.helps.collection.concurrent.AsyncEvents;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicLong;

import static janusgraph.util.batchimport.unsafe.helps.Exceptions.withMessage;
import static java.lang.String.format;

public class BadCollector implements Collector
{
    /**
     * Introduced to avoid creating an exception for every reported bad thing, since it can be
     * quite the performance hogger for scenarios where there are many many bad things to collect.
     */
    abstract static class ProblemReporter extends AsyncEvent
    {
        private final int type;

        ProblemReporter( int type )
        {
            this.type = type;
        }

        int type()
        {
            return type;
        }

        abstract String message();

        abstract Exception exception();
    }

    public static final int BAD_RELATIONSHIPS = 0x1;
    public static final int DUPLICATE_NODES = 0x2;
    public static final int EXTRA_COLUMNS = 0x4;
    public static final int COLLECT_ALL = BAD_RELATIONSHIPS | DUPLICATE_NODES | EXTRA_COLUMNS;
    public static final long UNLIMITED_TOLERANCE = -1;

    private final PrintStream out;
    private final long tolerance;
    private final int collect;
    private final boolean logBadEntries;

    // volatile since one importer thread calls collect(), where this value is incremented and later the "main"
    // thread calls badEntries() to get a count.
    private final AtomicLong badEntries = new AtomicLong();
    private final AsyncEvents<ProblemReporter> logger;
    private final Thread eventProcessor;

    public BadCollector(OutputStream out, long tolerance, int collect )
    {
        this( out, tolerance, collect, false );
    }

    public BadCollector(OutputStream out, long tolerance, int collect, boolean skipBadEntriesLogging )
    {
        this.out = new PrintStream( out );
        this.tolerance = tolerance;
        this.collect = collect;
        this.logBadEntries = !skipBadEntriesLogging;
        this.logger = new AsyncEvents<>( this::processEvent, AsyncEvents.Monitor.NONE );
        this.eventProcessor = new Thread( logger );
        this.eventProcessor.start();
    }

    private void processEvent( ProblemReporter report )
    {
        out.println( report.message() );
    }

    @Override
    public void collectBadRelationship( Object startId, String startIdGroup, String type, Object endId,
            String endIdGroup, Object specificValue ) throws Exception {
        collect( new RelationshipsProblemReporter( startId, startIdGroup, type, endId, endIdGroup, specificValue ) );
    }

    @Override
    public void collectExtraColumns( final String source, final long row, final String value ) throws Exception {
        collect( new ExtraColumnsProblemReporter( row, source, value ) );
    }

    @Override
    public void collectDuplicateNode( final Object id, long actualId, final String group ) throws Exception {
        collect( new NodesProblemReporter( id, group ) );
    }

    @Override
    public boolean isCollectingBadRelationships()
    {
        return collects( BAD_RELATIONSHIPS );
    }

    private void collect( ProblemReporter report ) throws Exception {
        boolean collect = collects( report.type() );
        if ( collect )
        {
            // This type of problem is collected and we're within the max threshold, so it's OK
            long count = badEntries.incrementAndGet();
            if ( tolerance == UNLIMITED_TOLERANCE || count <= tolerance )
            {
                // We're within the threshold
                if ( logBadEntries )
                {
                    // Send this to the logger
                    logger.send( report );
                }
                return; // i.e. don't treat this as an exception
            }
        }

        Exception exception = report.exception();
        throw collect ? withMessage( exception, format( "Too many bad entries %d, where last one was: %s",
                badEntries.longValue(), exception.getMessage() ) ) : exception;
    }

    @Override
    public void close()
    {
        logger.shutdown();
        try
        {
            logger.awaitTermination();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            out.flush();
        }
    }

    @Override
    public long badEntries()
    {
        return badEntries.get();
    }

    private boolean collects( int bit )
    {
        return (collect & bit) != 0;
    }

    private static class RelationshipsProblemReporter extends ProblemReporter
    {
        private String message;
        private final Object specificValue;
        private final Object startId;
        private final String startIdGroup;
        private final String type;
        private final Object endId;
        private final String endIdGroup;

        RelationshipsProblemReporter( Object startId, String startIdGroup, String type,
                Object endId, String endIdGroup, Object specificValue )
        {
            super( BAD_RELATIONSHIPS );
            this.startId = startId;
            this.startIdGroup = startIdGroup;
            this.type = type;
            this.endId = endId;
            this.endIdGroup = endIdGroup;
            this.specificValue = specificValue;
        }

        @Override
        public String message()
        {
            return getReportMessage();
        }

        @Override
        public Exception exception()
        {
            return new Exception( getReportMessage() );
        }

        private String getReportMessage()
        {
            if ( message == null )
            {
                message = !isMissingData()
                        ? format( "%s (%s)-[%s]->%s (%s) referring to missing node %s",
                                startId, startIdGroup, type, endId, endIdGroup, specificValue )
                        : format( "%s (%s)-[%s]->%s (%s) is missing data",
                                startId, startIdGroup, type, endId, endIdGroup );
            }
            return message;
        }

        private boolean isMissingData()
        {
            return startId == null || endId == null || type == null;
        }
    }

    private static class NodesProblemReporter extends ProblemReporter
    {
        private final Object id;
        private final String group;

        NodesProblemReporter( Object id, String group )
        {
            super( DUPLICATE_NODES );
            this.id = id;
            this.group = group;
        }

        @Override
        public String message()
        {
            return DuplicateInputIdException.message( id, group );
        }

        @Override
        public Exception exception()
        {
            return new DuplicateInputIdException( id, group );
        }
    }

    private static class ExtraColumnsProblemReporter extends ProblemReporter
    {
        private String message;
        private final long row;
        private final String source;
        private final String value;

        ExtraColumnsProblemReporter( long row, String source, String value )
        {
            super( EXTRA_COLUMNS );
            this.row = row;
            this.source = source;
            this.value = value;
        }

        @Override
        public String message()
        {
            return getReportMessage();
        }

        @Override
        public Exception exception()
        {
            return new Exception( getReportMessage() );
        }

        private String getReportMessage()
        {
            if ( message == null )
            {
                message = format( "Extra column not present in header on line %d in %s with value %s", row, source, value );
            }
            return message;
        }
    }
}
