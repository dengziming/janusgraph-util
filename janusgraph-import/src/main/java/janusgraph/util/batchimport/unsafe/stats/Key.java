package janusgraph.util.batchimport.unsafe.stats;

/**
 * {@link Stat Statistics} key. Has accessors for different types of information, like a name for identifying the key,
 * a short name for brief display and a description.
 */
public interface Key
{
    /**
     * Name that identifies this key.
     */
    String name();

    /**
     * Short name for outputs with tight space.
     */
    String shortName();

    /**
     * Longer description of what this key represents.
     */
    String description();
}
