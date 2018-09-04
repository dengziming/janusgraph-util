package janusgraph.util.batchimport.unsafe.stats;

/**
 * Ability to provide statistics.
 */
public interface StatsProvider
{
    Stat stat(Key key);

    Key[] keys();
}
