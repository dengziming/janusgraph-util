package janusgraph.util.batchimport.unsafe.lifecycle;

/**
* Enumerates the different status an instance can have while managed through CombineLifecycle.
*/
public enum LifecycleStatus
{
    NONE,
    INITIALIZING,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED,
    SHUTTING_DOWN,
    SHUTDOWN
}
