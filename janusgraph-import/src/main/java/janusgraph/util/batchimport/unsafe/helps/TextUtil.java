package janusgraph.util.batchimport.unsafe.helps;

import java.util.*;

public class TextUtil
{
    private TextUtil()
    {
    }

    public static String templateString( String templateString,
            Map<String, ? extends Object> data )
    {
        return templateString( templateString, "\\$", data );
    }

    public static String templateString( String templateString,
            String variablePrefix, Map<String, ? extends Object> data )
    {
        // Sort data strings on length.
        Map<Integer, List<String>> lengthMap =
            new HashMap<>();
        int longest = 0;
        for ( String key : data.keySet() )
        {
            int length = key.length();
            if ( length > longest )
            {
                longest = length;
            }

            List<String> innerList = null;
            Integer innerKey = Integer.valueOf( length );
            if ( lengthMap.containsKey( innerKey ) )
            {
                innerList = lengthMap.get( innerKey );
            }
            else
            {
                innerList = new ArrayList<>();
                lengthMap.put( innerKey, innerList );
            }
            innerList.add( key );
        }

        // Replace it.
        String result = templateString;
        for ( int i = longest; i >= 0; i-- )
        {
            Integer lengthKey = Integer.valueOf( i );
            if ( !lengthMap.containsKey( lengthKey ) )
            {
                continue;
            }

            List<String> list = lengthMap.get( lengthKey );
            for ( String key : list )
            {
                Object value = data.get( key );
                if ( value != null )
                {
                    String replacement = data.get( key ).toString();
                    String regExpMatchString = variablePrefix + key;
                    result = result.replaceAll( regExpMatchString, replacement );
                }
            }
        }

        return result;
    }

    public static String lastWordOrQuoteOf( String text, boolean preserveQuotation )
    {
        String[] quoteParts = text.split( "\"" );
        String lastPart = quoteParts[quoteParts.length - 1];
        boolean isWithinQuotes = quoteParts.length % 2 == 0;
        String lastWord = null;
        if ( isWithinQuotes )
        {
            lastWord = lastPart;
            if ( preserveQuotation )
            {
                lastWord = "\"" + lastWord + (text.endsWith( "\"" ) ? "\"" : "");
            }
        }
        else
        {
            String[] lastPartParts = splitAndKeepEscapedSpaces( lastPart, preserveQuotation );
            lastWord = lastPartParts[lastPartParts.length - 1];
        }
        return lastWord;
    }

    private static String[] splitAndKeepEscapedSpaces( String string, boolean preserveEscapes )
    {
        Collection<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for ( int i = 0; i < string.length(); i++ )
        {
            char ch = string.charAt( i );
            if ( ch == ' ' )
            {
                boolean isEscapedSpace = i > 0 && string.charAt( i - 1 ) == '\\';
                if ( !isEscapedSpace )
                {
                    result.add( current.toString() );
                    current = new StringBuilder();
                    continue;
                }
            }

            if ( preserveEscapes || ch != '\\' )
            {
                current.append( ch );
            }
        }
        if ( current.length() > 0 )
        {
            result.add( current.toString() );
        }
        return result.toArray( new String[result.size()] );
    }

    /**
     * Tokenizes a string, regarding quotes.
     *
     * @param string the string to tokenize.
     * @return the tokens from the line.
     */
    public static String[] tokenizeStringWithQuotes( String string )
    {
        return tokenizeStringWithQuotes( string, true, false );
    }

    /**
     * Tokenizes a string, regarding quotes. Examples:
     *
     * o '"One two"'              ==&gt; [ "One two" ]
     * o 'One two'                ==&gt; [ "One", "two" ]
     * o 'One "two three" four'   ==&gt; [ "One", "two three", "four" ]
     *
     * @param string the string to tokenize.
     * @param trim  whether or not to trim each token.
     * @param preserveEscapeCharacters whether or not to preserve escape characters '\', otherwise skip them.
     * @return the tokens from the line.
     */
    public static String[] tokenizeStringWithQuotes( String string, boolean trim, boolean preserveEscapeCharacters )
    {
        if ( trim )
        {
            string = string.trim();
        }
        ArrayList<String> result = new ArrayList<>();
        string = string.trim();
        boolean inside = string.startsWith( "\"" );
        StringTokenizer quoteTokenizer = new StringTokenizer( string, "\"" );
        while ( quoteTokenizer.hasMoreTokens() )
        {
            String token = quoteTokenizer.nextToken();
            if ( trim )
            {
                token = token.trim();
            }
            if ( token.length() == 0 )
            {
                // Skip it
            }
            else if ( inside )
            {
                // Don't split
                result.add( token );
            }
            else
            {
                Collections.addAll( result, TextUtil.splitAndKeepEscapedSpaces( token, preserveEscapeCharacters ) );
            }
            inside = !inside;
        }
        return result.toArray( new String[result.size()] );
    }
}
