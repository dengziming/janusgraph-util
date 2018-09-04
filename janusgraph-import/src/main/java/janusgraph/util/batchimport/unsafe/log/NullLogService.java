package janusgraph.util.batchimport.unsafe.log;


public class NullLogService implements LogService
{
    private static final NullLogService INSTANCE = new NullLogService();

    public final NullLogProvider nullLogProvider = NullLogProvider.getInstance();
    public final NullLog nullLog = NullLog.getInstance();

    private NullLogService()
    {
    }

    public static NullLogService getInstance()
    {
        return INSTANCE;
    }

    @Override
    public LogProvider getUserLogProvider()
    {
        return nullLogProvider;
    }

    @Override
    public Log getUserLog( Class loggingClass )
    {
        return nullLog;
    }

    @Override
    public LogProvider getInternalLogProvider()
    {
        return nullLogProvider;
    }

    @Override
    public Log getInternalLog( Class loggingClass )
    {
        return nullLog;
    }
}
