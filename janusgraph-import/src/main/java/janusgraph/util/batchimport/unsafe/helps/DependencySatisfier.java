package janusgraph.util.batchimport.unsafe.helps;

/**
 * Able to satisfy dependencies, later needed to be resolved by for example a {@link DependencyResolver}.
 */
public interface DependencySatisfier
{
    <T> T satisfyDependency(T dependency);
}
