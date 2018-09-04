package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.Configuration;

import java.util.ArrayList;
import java.util.List;

import static janusgraph.util.batchimport.unsafe.helps.Exceptions.launderedException;


/**
 * A stage of processing, mainly consisting of one or more {@link Step steps} that batches of data to
 * process flows through.
 */
public class Stage
{
    private final List<Step<?>> pipeline = new ArrayList<>();
    private final StageExecution execution;

    public Stage(String name, String part, Configuration config, int orderingGuarantees )
    {
        this.execution = new StageExecution( name, part, config, pipeline, orderingGuarantees );
    }

    protected StageControl control()
    {
        return execution;
    }

    public void add( Step<?> step )
    {
        pipeline.add( step );
    }

    public StageExecution execute()
    {
        linkSteps();
        execution.start();
        pipeline.get( 0 ).receive( 1 /*a ticket, ignored anyway*/, null /*serves only as a start signal anyway*/ );
        return execution;
    }

    private void linkSteps()
    {
        Step<?> previous = null;
        for ( Step<?> step : pipeline )
        {
            if ( previous != null )
            {
                previous.setDownstream( step );
            }
            previous = step;
        }
    }

    public void close()
    {
        Exception exception = null;
        for ( Step<?> step : pipeline )
        {
            try
            {
                step.close();
            }
            catch ( Exception e )
            {
                if ( exception == null )
                {
                    exception = e;
                }
                else
                {
                    exception.addSuppressed( e );
                }
            }
        }
        execution.close();
        if ( exception != null )
        {
            throw launderedException( exception );
        }
    }

    @Override
    public String toString()
    {
        return execution.getStageName();
    }
}
