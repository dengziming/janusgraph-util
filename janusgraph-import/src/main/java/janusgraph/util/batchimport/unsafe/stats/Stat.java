package janusgraph.util.batchimport.unsafe.stats;

/**
 * Statistic about a particular thing.
 */
public interface Stat
{
    DetailLevel detailLevel();

    long asLong();
}
