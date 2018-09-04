package janusgraph.util.batchimport.unsafe.helps;

import javax.annotation.Nonnull;

/**
 * Super class for exceptions stemming from invalid format of a data source that is read.
 */
public abstract class FormatException extends IllegalStateException
{
    private final SourceTraceability source;

    protected FormatException(@Nonnull SourceTraceability source, @Nonnull String description )
    {
        super( "At " + source.sourceDescription() + " @ position " + source.position() + " - " + description );
        this.source = source;
    }

    public SourceTraceability source()
    {
        return this.source;
    }
}
