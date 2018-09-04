package janusgraph.util.batchimport.unsafe.helps;

import static java.lang.String.format;

public class IllegalMultilineFieldException extends FormatException
{
    public IllegalMultilineFieldException(SourceTraceability source )
    {
        super( source, format( "Multi-line fields are illegal in this context and so this might suggest that " +
                "there's a field with a start quote, but a missing end quote. See %s @ position %d.",
                        source.sourceDescription(), source.position() ) );
    }
}
