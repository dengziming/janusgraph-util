package janusgraph.util.batchimport.unsafe.log;

/**
 * Used to obtain a {@link Log} for a specified context
 */
public interface LogProvider
{
    /**
     * @param loggingClass the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the {@code loggingClass} as the context
     */
    Log getLog(Class loggingClass);

    /**
     * @param name the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the specified name as the context
     */
    Log getLog(String name);
}
