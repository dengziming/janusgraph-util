package janusgraph.util.batchimport.unsafe.input;

import java.util.*;

import static java.util.Arrays.asList;

/**
 * Mapping from name to {@link Group}. Assigns proper {@link Group#id() ids} to created groups.
 */
public class Groups
{
    static final int LOWEST_NONGLOBAL_ID = 1;

    private final Map<String,Group> byName = new HashMap<>();
    private final List<Group> byId = new ArrayList<>( asList( Group.GLOBAL ) );
    private int nextId = LOWEST_NONGLOBAL_ID;

    /**
     * @param name group name or {@code null} for a {@link Group#GLOBAL global group}.
     * @return {@link Group} for the given name. If the group doesn't already exist it will be created
     * with a new id. If {@code name} is {@code null} then the {@link Group#GLOBAL global group} is returned.
     * This method also prevents mixing global and non-global groups, i.e. if first call is {@code null},
     * then consecutive calls have to specify {@code null} name as well. The same holds true for non-null values.
     */
    public synchronized Group getOrCreate( String name )
    {
        if ( isGlobalGroup( name ) )
        {
            return Group.GLOBAL;
        }

        Group group = byName.get( name );
        if ( group == null )
        {
            byName.put( name, group = new Group.Adapter( nextId++, name ) );
            byId.add( group );
        }
        return group;
    }

    private static boolean isGlobalGroup( String name )
    {
        return name == null || Group.GLOBAL.name().equals( name );
    }

    public synchronized Group get( String name )
    {
        if ( isGlobalGroup( name ) )
        {
            return Group.GLOBAL;
        }

        Group group = byName.get( name );
        if ( group == null )
        {
            try {
                throw new Exception( "Group '" + name + "' not found. Available groups are: " + groupNames() );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return group;
    }

    public Group get( int id )
    {
        if ( id < 0 || id >= byId.size() )
        {
            try {
                throw new Exception( "Group with id " + id + " not found" );
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return byId.get( id );
    }

    private String groupNames()
    {
        return Arrays.toString( byName.keySet().toArray( new String[byName.keySet().size()] ) );
    }

    public int size()
    {
        return nextId;
    }
}
