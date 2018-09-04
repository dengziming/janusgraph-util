package janusgraph.util.batchimport.unsafe.helps;


import janusgraph.util.batchimport.unsafe.input.csv.Configuration;

import java.util.function.Function;

/**
 * Converts a string expression into a character to be used as delimiter, array delimiter, or quote character. Can be
 * normal characters as well as for example: '\t', '\123', and "TAB".
 */
public class CharacterConverter implements Function<String,Character>
{

    @Override
    public Character apply( String value ) throws RuntimeException
    {
        // Parse "raw" ASCII character style characters:
        // - \123 --> character with id 123
        // - \t   --> tab character
        if ( value.startsWith( "\\" ) && value.length() > 1 )
        {
            String raw = value.substring( 1 );
            try
            {
                return (char) Integer.parseInt( raw );
            }
            catch ( NumberFormatException e )
            {
                if ( raw.equals( "t" ) )
                {
                    return Configuration.TABS.delimiter();
                }
            }
        }
        // hard coded TAB --> tab character
        else if ( value.equals( "TAB" ) )
        {
            return Configuration.TABS.delimiter();
        }
        else if ( value.length() == 1 )
        {
            return value.charAt( 0 );
        }

        throw new IllegalArgumentException( "Unsupported character '" + value + "'" );
    }
}
