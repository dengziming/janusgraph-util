package janusgraph.util.batchimport.unsafe.progress;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public abstract class ProgressMonitorFactory
{
    public static final ProgressMonitorFactory NONE = new ProgressMonitorFactory()
    {
        @Override
        protected Indicator newIndicator( String process )
        {
            return Indicator.NONE;
        }
    };

    public static ProgressMonitorFactory textual( final OutputStream out )
    {
        return textual( new OutputStreamWriter( out, StandardCharsets.UTF_8 ) );
    }

    public static ProgressMonitorFactory textual( final Writer out )
    {
        return new ProgressMonitorFactory()
        {
            @Override
            protected Indicator newIndicator( String process )
            {
                return new Indicator.Textual( process, writer() );
            }

            private PrintWriter writer()
            {
                return out instanceof PrintWriter ? (PrintWriter) out : new PrintWriter( out );
            }
        };
    }

    public final MultiPartBuilder multipleParts( String process )
    {
        return new MultiPartBuilder( this, process );
    }

    public final ProgressListener singlePart( String process, long totalCount )
    {
        return new ProgressListener.SinglePartProgressListener( newIndicator( process ), totalCount );
    }

    protected abstract Indicator newIndicator( String process );

    public static class MultiPartBuilder
    {
        private Aggregator aggregator;
        private Set<String> parts = new HashSet<>();

        private MultiPartBuilder( ProgressMonitorFactory factory, String process )
        {
            this.aggregator = new Aggregator( factory.newIndicator( process ) );
        }

        public ProgressListener progressForPart( String part, long totalCount )
        {
            assertNotBuilt();
            assertUniquePart( part );
            ProgressListener.MultiPartProgressListener progress =
                    new ProgressListener.MultiPartProgressListener( aggregator, part, totalCount );
            aggregator.add( progress, totalCount );
            return progress;
        }

        public ProgressListener progressForUnknownPart( String part )
        {
            assertNotBuilt();
            assertUniquePart( part );
            ProgressListener progress = ProgressListener.NONE;
            aggregator.add( progress, 0 );
            return progress;
        }

        private void assertUniquePart( String part )
        {
            if ( !parts.add( part ) )
            {
                throw new IllegalArgumentException( String.format( "Part '%s' has already been defined.", part ) );
            }
        }

        private void assertNotBuilt()
        {
            if ( aggregator == null )
            {
                throw new IllegalStateException( "Builder has been completed." );
            }
        }

        public void build()
        {
            if ( aggregator != null )
            {
                aggregator.initialize();
            }
            aggregator = null;
            parts = null;
        }
    }
}
