package janusgraph.util.batchimport.unsafe.helps;

public class UnsatisfiedDependencyException extends RuntimeException
{
    public UnsatisfiedDependencyException(Throwable cause )
    {
        super( cause );
    }

    public UnsatisfiedDependencyException(Class<?> type )
    {
        super( "No dependency satisfies type " + type );
    }
}
