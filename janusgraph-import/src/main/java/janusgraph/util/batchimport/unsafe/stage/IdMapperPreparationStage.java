package janusgraph.util.batchimport.unsafe.stage;



import janusgraph.util.batchimport.unsafe.Configuration;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.progress.ProgressListener;
import janusgraph.util.batchimport.unsafe.stats.StatsProvider;

import java.util.function.LongFunction;

/**
 * Performs {@link IdMapper#prepare(LongFunction, Collector, ProgressListener)}
 * embedded in a {@link Stage} as to take advantage of statistics and monitoring provided by that framework.
 */
public class IdMapperPreparationStage extends Stage
{
    public static final String NAME = "Prepare node index";

    public IdMapperPreparationStage(Configuration config, IdMapper idMapper,
                                    Collector collector, StatsProvider memoryUsageStats )
    {
        super( NAME, null, config, 0 );
        add( new IdMapperPreparationStep( control(), config, idMapper, collector, memoryUsageStats ) );
    }
}
