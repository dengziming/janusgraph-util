package janusgraph.util.batchimport.unsafe.progress;

import java.io.PrintWriter;

public abstract class Indicator
{
    static final Indicator NONE = new Indicator( 1 )
    {
        @Override
        protected void progress( int from, int to )
        {
        }
    };

    private final int reportResolution;

    public Indicator( int reportResolution )
    {
        this.reportResolution = reportResolution;
    }

    protected abstract void progress( int from, int to );

    int reportResolution()
    {
        return reportResolution;
    }

    public void startProcess( long totalCount )
    {
    }

    public void startPart( String part, long totalCount )
    {
    }

    public void completePart( String part )
    {
    }

    public void completeProcess()
    {
    }

    public void failure( Throwable cause )
    {
    }

    static class Textual extends Indicator
    {
        private final String process;
        private final PrintWriter out;

        Textual( String process, PrintWriter out )
        {
            super( 200 );
            this.process = process;
            this.out = out;
        }

        @Override
        public void startProcess( long totalCount )
        {
            out.println( process );
            out.flush();
        }

        @Override
        protected void progress( int from, int to )
        {
            for ( int i = from; i < to; )
            {
                printProgress( ++i );
            }
            out.flush();
        }

        @Override
        public void failure( Throwable cause )
        {
            cause.printStackTrace( out );
        }

        private void printProgress( int progress )
        {
            out.print( '.' );
            if ( progress % 20 == 0 )
            {
                out.printf( " %3d%%%n", progress / 2 );
            }
        }
    }
}
