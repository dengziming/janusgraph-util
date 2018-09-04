package janusgraph.util.batchimport.unsafe.log;

/**
 * A {@link LogProvider} implementation that discards all messages
 */
public class NullLogProvider implements LogProvider
{
    private static final NullLogProvider INSTANCE = new NullLogProvider();

    private NullLogProvider()
    {
    }

    /**
     * @return A singleton {@link NullLogProvider} instance
     */
    public static NullLogProvider getInstance()
    {
        return INSTANCE;
    }

    @Override
    public Log getLog( Class loggingClass )
    {
        return NullLog.getInstance();
    }

    @Override
    public Log getLog( String name )
    {
        return NullLog.getInstance();
    }
}
