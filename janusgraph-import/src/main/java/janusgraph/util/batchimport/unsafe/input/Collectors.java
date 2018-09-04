package janusgraph.util.batchimport.unsafe.input;


import org.apache.commons.io.output.NullOutputStream;

import java.io.OutputStream;
import java.util.function.Function;

/**
 * Common implementations of {@link Collector}
 */
public class Collectors
{
    private Collectors()
    {
    }

    public static Collector silentBadCollector( long tolerance )
    {
        return silentBadCollector( tolerance, BadCollector.COLLECT_ALL );
    }

    public static Collector silentBadCollector( long tolerance, int collect )
    {
        return badCollector( NullOutputStream.NULL_OUTPUT_STREAM, tolerance, collect );
    }

    public static Collector badCollector( OutputStream out, long unlimitedTolerance )
    {
        return badCollector( out, unlimitedTolerance, BadCollector.COLLECT_ALL, false );
    }

    public static Collector badCollector( OutputStream out, long tolerance, int collect )
    {
        return new BadCollector( out, tolerance, collect, false );
    }

    public static Collector badCollector( OutputStream out, long unlimitedTolerance, int collect, boolean skipBadEntriesLogging )
    {
        return new BadCollector( out, unlimitedTolerance, collect, skipBadEntriesLogging );
    }

    public static Function<OutputStream,Collector> badCollector( final int tolerance )
    {
        return badCollector( tolerance, BadCollector.COLLECT_ALL );
    }

    public static Function<OutputStream,Collector> badCollector( final int tolerance, final int collect )
    {
        return out -> badCollector( out, tolerance, collect, false );
    }

    public static int collect( boolean skipBadRelationships, boolean skipDuplicateNodes, boolean ignoreExtraColumns )
    {
        return (skipBadRelationships ? BadCollector.BAD_RELATIONSHIPS : 0 ) |
               (skipDuplicateNodes ? BadCollector.DUPLICATE_NODES : 0 ) |
               (ignoreExtraColumns ? BadCollector.EXTRA_COLUMNS : 0 );
    }
}
