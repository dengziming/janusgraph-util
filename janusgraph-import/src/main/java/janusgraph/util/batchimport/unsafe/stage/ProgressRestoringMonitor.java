package janusgraph.util.batchimport.unsafe.stage;


/**
 * Monitor connecting a {@link HumanUnderstandableExecutionMonitor} and {@link OnDemandDetailsExecutionMonitor},
 * their monitors at least for the sole purpose of notifying {@link HumanUnderstandableExecutionMonitor} about when
 * there are other output interfering with it's nice progress printing. If something else gets printed it can restore its
 * progress from 0..current.
 */
public class ProgressRestoringMonitor implements HumanUnderstandableExecutionMonitor.ExternalMonitor, OnDemandDetailsExecutionMonitor.Monitor
{
    private volatile boolean detailsPrinted;

    @Override
    public void detailsPrinted()
    {
        this.detailsPrinted = true;
    }

    @Override
    public boolean somethingElseBrokeMyNiceOutput()
    {
        if ( detailsPrinted )
        {
            detailsPrinted = false;
            return true;
        }
        return false;
    }
}
