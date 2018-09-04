package janusgraph.util.batchimport.unsafe.stage;


import janusgraph.util.batchimport.unsafe.Configuration;

/**
 * Convenience around executing and supervising {@link Stage stages}.
 */
public class ExecutionSupervisors
{
    private ExecutionSupervisors()
    {
    }

    /**
     * Using an {@link ExecutionMonitors#invisible() invisible} monitor.
     * @param stage {@link Stage} to supervise.
     * @see #superviseDynamicExecution(ExecutionMonitor, Stage)
     */
    public static void superviseDynamicExecution( Stage stage )
    {
        superviseDynamicExecution( ExecutionMonitors.invisible(), stage );
    }

    /**
     * With {@link Configuration#DEFAULT}.
     * @param monitor {@link ExecutionMonitor} notifying user about progress.
     * @param stage {@link Stage} to supervise.
     * @see #superviseDynamicExecution(ExecutionMonitor, Configuration, Stage)
     */
    public static void superviseDynamicExecution( ExecutionMonitor monitor, Stage stage )
    {
        superviseDynamicExecution( monitor, Configuration.DEFAULT, stage );
    }

    /**
     * Supervises an execution with the given monitor AND a {@link DynamicProcessorAssigner} to give
     * the execution a dynamic and optimal nature.
     * @param monitor {@link ExecutionMonitor} notifying user about progress.
     * @param config {@link Configuration} of the import.
     * @param stage {@link Stage} to supervise.
     *
     * @see #superviseExecution(ExecutionMonitor, Configuration, Stage)
     */
    public static void superviseDynamicExecution( ExecutionMonitor monitor, Configuration config, Stage stage )
    {
        superviseExecution( withDynamicProcessorAssignment( monitor, config ), config, stage );
    }

    /**
     * Executes a number of stages simultaneously, letting the given {@code monitor} get insight into the
     * execution.
     *
     * @param monitor {@link ExecutionMonitor} to get insight into the execution.
     * @param config {@link Configuration} for the execution.
     * @param stage {@link Stage stages} to execute.
     */
    public static void superviseExecution( ExecutionMonitor monitor, Configuration config, Stage stage )
    {
        ExecutionSupervisor supervisor = new ExecutionSupervisor( Clocks.systemClock(), monitor );
        StageExecution execution = null;
        try
        {
            execution = stage.execute();
            supervisor.supervise( execution );
        }
        finally
        {
            stage.close();
            if ( execution != null )
            {
                execution.assertHealthy();
            }
        }
    }

    /**
     * Decorates an {@link ExecutionMonitor} with a {@link DynamicProcessorAssigner} responsible for
     * constantly assigning and reevaluating an optimal number of processors to all individual steps.
     *
     * @param monitor {@link ExecutionMonitor} to decorate.
     * @param config {@link Configuration} that the {@link DynamicProcessorAssigner} will use. Max total processors
     * in a {@link Stage} will be the smallest of that value and {@link Runtime#availableProcessors()}.
     * @return the decorated monitor with dynamic processor assignment capabilities.
     */
    public static ExecutionMonitor withDynamicProcessorAssignment( ExecutionMonitor monitor, Configuration config )
    {
        DynamicProcessorAssigner dynamicProcessorAssigner = new DynamicProcessorAssigner( config );
        return new MultiExecutionMonitor( monitor, dynamicProcessorAssigner );
    }
}
