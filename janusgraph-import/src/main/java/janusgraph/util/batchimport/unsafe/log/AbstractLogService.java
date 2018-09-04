package janusgraph.util.batchimport.unsafe.log;

/**
 * Logging service that is used to obtain loggers for different output purposes
 */
public abstract class AbstractLogService implements LogService
{
    @Override
    public abstract LogProvider getUserLogProvider();

    @Override
    public Log getUserLog( Class loggingClass )
    {
        return getUserLogProvider().getLog( loggingClass );
    }

    @Override
    public abstract LogProvider getInternalLogProvider();

    @Override
    public Log getInternalLog( Class loggingClass )
    {
        return getInternalLogProvider().getLog( loggingClass );
    }
}
