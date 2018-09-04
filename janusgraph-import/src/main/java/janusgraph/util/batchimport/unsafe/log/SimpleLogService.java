package janusgraph.util.batchimport.unsafe.log;


public class SimpleLogService extends AbstractLogService
{
    private final LogProvider userLogProvider;
    private final LogProvider internalLogProvider;

    /**
     * Create log service where both: user and internal log provider use the same {@link LogProvider} as a provider.
     * Should be used when user and internal are backed by same log provider.
     * @param commonLogProvider log provider
     */
    public SimpleLogService( LogProvider commonLogProvider )
    {
        this.userLogProvider = commonLogProvider;
        this.internalLogProvider = commonLogProvider;
    }

    /**
     * Create log service with different user and internal log providers.
     * User logs will be duplicated to internal logs as well.
     * Should be used when user and internal are backed by different log providers.
     * @param userLogProvider user log provider
     * @param internalLogProvider internal log provider
     */
    public SimpleLogService( LogProvider userLogProvider, LogProvider internalLogProvider )
    {
        this.userLogProvider = new DuplicatingLogProvider( userLogProvider, internalLogProvider );
        this.internalLogProvider = internalLogProvider;
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return this.userLogProvider;
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return this.internalLogProvider;
    }
}
