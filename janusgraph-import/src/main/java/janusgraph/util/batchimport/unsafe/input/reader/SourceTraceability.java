package janusgraph.util.batchimport.unsafe.input.reader;

/**
 * Provides information about a source of data.
 *
 * An example usage would be reading a text file where {@link #sourceDescription()} would say the name of the file,
 * and {@link #position()} the byte offset the reader is currently at.
 *
 * Another example could be reading from a relationship db table where {@link #sourceDescription()} would
 * say the name of the database and table and {@link #position()} some sort of absolute position saying
 * the byte offset to the field.
 */
public interface SourceTraceability
{
    /**
     * @return description of the source being read from.
     */
    String sourceDescription();

    /**
     * @return a low-level byte-like position e.g. byte offset.
     */
    long position();

    abstract class Adapter implements SourceTraceability
    {
        @Override
        public long position()
        {
            return 0;
        }
    }

    SourceTraceability EMPTY = new Adapter()
    {
        @Override
        public String sourceDescription()
        {
            return "EMPTY";
        }
    };
}
