package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;

import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;

import java.util.Arrays;

/**
 * Used by {@link EncodingIdMapper} to help detect collisions of encoded values within the same group.
 * Same values for different groups are not considered collisions.
 */
class SameGroupDetector
{
    // Alternating data index, group id
    private long[] seen = new long[100]; // grows on demand
    private int cursor;

    /**
     * @return -1 if no collision within the same group, or an actual data index which collides with the
     * supplied data index and group id. In the case of <strong>not</strong> {@code -1} both {@code dataIndexB}
     * and the returned data index should be marked as collisions.
     */
    long collisionWithinSameGroup( long dataIndexA, int groupIdA, long dataIndexB, int groupIdB )
    {
        // The first call, add both the entries. For consecutive calls for this same collision stretch
        // only add and compare the second. The reason it's done in here instead of having a method signature
        // of one data index and group id and having the caller call two times is that we're better suited
        // to decide if this is the first or consecutive call for this collision stretch.
        if ( cursor == 0 )
        {
            add( dataIndexA, groupIdA );
        }

        long collision = IdMapper.ID_NOT_FOUND;
        for ( int i = 0; i < cursor; i++ )
        {
            long dataIndexAtCursor = seen[i++];
            long groupIdAtCursor = seen[i];
            if ( groupIdAtCursor == groupIdB )
            {
                collision = dataIndexAtCursor;
                break;
            }
        }

        add( dataIndexB, groupIdB );

        return collision;
    }

    private void add( long dataIndex, int groupId )
    {
        if ( cursor == seen.length )
        {
            seen = Arrays.copyOf( seen, seen.length * 2 );
        }
        seen[cursor++] = dataIndex;
        seen[cursor++] = groupId;
    }

    void reset()
    {
        cursor = 0;
    }
}
