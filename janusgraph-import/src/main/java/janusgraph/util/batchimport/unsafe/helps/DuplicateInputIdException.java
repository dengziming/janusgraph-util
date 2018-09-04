package janusgraph.util.batchimport.unsafe.helps;


import static java.lang.String.format;

public class DuplicateInputIdException extends Exception
{
    public DuplicateInputIdException(Object id, String groupName )
    {
        super( message( id, groupName ) );
    }

    public static String message( Object id, String groupName )
    {
        return format( "Id '%s' is defined more than once in group '%s'", id, groupName );
    }
}
