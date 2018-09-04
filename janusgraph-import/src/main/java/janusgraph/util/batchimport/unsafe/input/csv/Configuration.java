package janusgraph.util.batchimport.unsafe.input.csv;

/**
 * Configuration for {@link CsvInput}.
 */
public interface Configuration extends janusgraph.util.batchimport.unsafe.input.reader.Configuration
{
    /**
     * Delimiting character between each values in a CSV input line.
     * Typical character is '\t' (TAB) or ',' (it is Comma Separated Values after all).
     */
    char delimiter();

    /**
     * Character separating array values from one another for values that represent arrays.
     */
    char arrayDelimiter();

    abstract class Default extends janusgraph.util.batchimport.unsafe.input.reader.Configuration.Default implements Configuration
    {
    }

    Configuration COMMAS = new Default()
    {
        @Override
        public char delimiter()
        {
            return ',';
        }

        @Override
        public char arrayDelimiter()
        {
            return ';';
        }
    };

    Configuration TABS = new Default()
    {
        @Override
        public char delimiter()
        {
            return '\t';
        }

        @Override
        public char arrayDelimiter()
        {
            return ',';
        }
    };

    class Overridden extends janusgraph.util.batchimport.unsafe.input.reader.Configuration.Overridden implements Configuration
    {
        private final Configuration defaults;

        public Overridden( Configuration defaults )
        {
            super( defaults );
            this.defaults = defaults;
        }

        @Override
        public char delimiter()
        {
            return defaults.delimiter();
        }

        @Override
        public char arrayDelimiter()
        {
            return defaults.arrayDelimiter();
        }
    }
}
