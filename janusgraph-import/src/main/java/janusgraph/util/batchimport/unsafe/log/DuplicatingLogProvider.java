package janusgraph.util.batchimport.unsafe.log;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;

/**
 * A {@link LogProvider} implementation that duplicates all messages to other LogProvider instances
 */
public class DuplicatingLogProvider extends AbstractLogProvider<DuplicatingLog>
{
    private final CopyOnWriteArraySet<LogProvider> logProviders;
    private final Map<DuplicatingLog,Map<LogProvider,Log>> duplicatingLogCache =
            Collections.synchronizedMap( new WeakHashMap<DuplicatingLog,Map<LogProvider,Log>>() );

    /**
     * @param logProviders A list of {@link LogProvider} instances that messages should be duplicated to
     */
    public DuplicatingLogProvider( LogProvider... logProviders )
    {
        this.logProviders = new CopyOnWriteArraySet<>( Arrays.asList( logProviders ) );
    }

    /**
     * Remove a {@link LogProvider} from the duplicating set. Note that the LogProvider must return
     * cached Log instances from its {@link LogProvider#getLog(String)} for this to behave as expected.
     *
     * @param logProvider the LogProvider to be removed
     * @return true if the log was found and removed
     */
    public boolean remove( LogProvider logProvider )
    {
        if ( !this.logProviders.remove( logProvider ) )
        {
            return false;
        }
        for ( DuplicatingLog duplicatingLog : cachedLogs() )
        {
            duplicatingLog.remove( duplicatingLogCache.get( duplicatingLog ).remove( logProvider ) );
        }
        return true;
    }

    @Override
    protected DuplicatingLog buildLog( final Class loggingClass )
    {
        return buildLog( logProvider -> logProvider.getLog( loggingClass ) );
    }

    @Override
    protected DuplicatingLog buildLog( final String name )
    {
        return buildLog( logProvider -> logProvider.getLog( name ) );
    }

    private DuplicatingLog buildLog( Function<LogProvider, Log> logConstructor )
    {
        ArrayList<Log> logs = new ArrayList<>( logProviders.size() );
        HashMap<LogProvider, Log> providedLogs = new HashMap<>();
        for ( LogProvider logProvider : logProviders )
        {
            Log log = logConstructor.apply( logProvider );
            providedLogs.put( logProvider, log );
            logs.add( log );
        }
        DuplicatingLog duplicatingLog = new DuplicatingLog( logs );
        duplicatingLogCache.put( duplicatingLog, providedLogs );
        return duplicatingLog;
    }
}
