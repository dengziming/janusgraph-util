package janusgraph.util.batchimport.unsafe.log;


/**
 * Logging service that is used to obtain loggers for different output purposes
 */
public interface LogService
{
    /**
     * @return a {@link LogProvider} that providers loggers for user visible messages.
     */
    LogProvider getUserLogProvider();

    /**
     * Equivalent to {@code {@link #getUserLogProvider}()( loggingClass )}
     * @param loggingClass the context for the return logger.
     * @return a {@link Log} that logs user visible messages with the {@code loggingClass} as context.
     */
    Log getUserLog(Class loggingClass);

    /**
     * @return a {@link LogProvider} that providers loggers for internal messages.
     */
    LogProvider getInternalLogProvider();

    /**
     * Equivalent to {@code {@link #getInternalLogProvider}()( loggingClass )}
     * @param loggingClass the context for the return logger.
     * @return a {@link Log} that logs internal messages with the {@code loggingClass} as context.
     */
    Log getInternalLog(Class loggingClass);
}
