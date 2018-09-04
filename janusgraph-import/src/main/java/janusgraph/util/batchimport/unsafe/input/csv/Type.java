package janusgraph.util.batchimport.unsafe.input.csv;


import org.janusgraph.diskstorage.Entry;

/**
 * Types used to semantically specify the contents of a {@link Entry}.
 */
public enum Type
{
    ID,
    PROPERTY,
    LABEL,
    TYPE,
    START_ID,
    END_ID,
    IGNORE
}
