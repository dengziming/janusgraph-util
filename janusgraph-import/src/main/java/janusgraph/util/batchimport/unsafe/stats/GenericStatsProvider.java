package janusgraph.util.batchimport.unsafe.stats;


import janusgraph.util.batchimport.unsafe.helps.Pair;

import java.util.ArrayList;
import java.util.Collection;

import static java.lang.String.format;

/**
 * Generic implementation for providing {@link Stat statistics}.
 */
public class GenericStatsProvider implements StatsProvider
{
    private final Collection<Pair<Key,Stat>> stats = new ArrayList<>();

    public void add( Key key, Stat stat )
    {
        this.stats.add( Pair.of( key, stat ) );
    }

    @Override
    public Stat stat( Key key )
    {
        for ( Pair<Key,Stat> stat1 : stats )
        {
            if ( stat1.first().name().equals( key.name() ) )
            {
                return stat1.other();
            }
        }
        return null;
    }

    @Override
    public Key[] keys()
    {
        Key[] keys = new Key[stats.size()];
        int i = 0;
        for ( Pair<Key,Stat> stat : stats )
        {
            keys[i++] = stat.first();
        }
        return keys;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for ( Pair<Key,Stat> stat : stats )
        {
            builder.append( builder.length() > 0 ? ", " : "" )
                    .append( format( "%s: %s", stat.first().shortName(), stat.other() ) );
        }
        return builder.toString();
    }


}
