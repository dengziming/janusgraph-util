package janusgraph.util.batchimport.unsafe.stage;

import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.progress.ProgressListener;
import janusgraph.util.batchimport.unsafe.stats.StatsProvider;

import java.util.function.LongFunction;

/**
 * Preparation of an {@link IdMapper}, {@link IdMapper#prepare(LongFunction, Collector, ProgressListener)}
 * under running as a normal {@link Step} so that normal execution monitoring can be applied.
 * Useful since preparing an {@link IdMapper} can take a significant amount of time.
 */
public class IdMapperPreparationStep extends LonelyProcessingStep
{
    private final IdMapper idMapper;
    private final Collector collector;

    public IdMapperPreparationStep(StageControl control, Configuration config,
                                   IdMapper idMapper,  Collector collector,
                                   StatsProvider... additionalStatsProviders )
    {
        super( control, "" /*named later in the progress listener*/, config, additionalStatsProviders );
        this.idMapper = idMapper;
        this.collector = collector;
    }

    @Override
    protected void process() throws Exception {
        idMapper.prepare( null, collector, new ProgressListener.Adapter()
        {
            @Override
            public void started( String task )
            {
                changeName( task );
            }

            @Override
            public void set( long progress )
            {
                throw new UnsupportedOperationException( "Shouldn't be required" );
            }

            @Override
            public void failed( Throwable e )
            {
                issuePanic( e );
            }

            @Override
            public synchronized void add( long progress )
            {   // Directly feed into the progress of this step.
                // Expected to be called by multiple threads, although quite rarely,
                // so synchronization overhead should be negligible.
                progress( progress );
            }

            @Override
            public void done()
            {   // Nothing to do
            }
        } );
    }
}
