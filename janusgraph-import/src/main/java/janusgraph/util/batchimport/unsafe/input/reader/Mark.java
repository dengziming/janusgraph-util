package janusgraph.util.batchimport.unsafe.input.reader;

import static java.lang.String.format;

/**
 * A mutable marker that is changed to hold progress made to a {@link BufferedCharSeeker}.
 * It holds information such as start/end position in the data stream, which character
 * was the match and whether or not this denotes the last value of the line.
 */
public class Mark
{
    public static int END_OF_LINE_CHARACTER = -1;

    private int startPosition;
    private int position;
    private int character;
    private boolean quoted;

    /**
     * @param startPosition position of first character in value (inclusive).
     * @param position position of last character in value (exclusive).
     * @param character use {@code -1} to denote that the matching character was an end-of-line or end-of-file
     * @param quoted whether or not the original data was quoted.
     */
    void set( int startPosition, int position, int character, boolean quoted )
    {
        this.startPosition = startPosition;
        this.position = position;
        this.character = character;
        this.quoted = quoted;
    }

    public int character()
    {
        assert !isEndOfLine();
        return character;
    }

    public boolean isEndOfLine()
    {
        return character == -1;
    }

    public boolean isQuoted()
    {
        return quoted;
    }

    int position()
    {
        if ( position == -1 )
        {
            throw new IllegalStateException( "No value to extract here" );
        }
        return position;
    }

    int startPosition()
    {
        if ( startPosition == -1 )
        {
            throw new IllegalStateException( "No value to extract here" );
        }
        return startPosition;
    }

    int length()
    {
        return position - startPosition;
    }

    @Override
    public String toString()
    {
        return format( "Mark[from:%d, to:%d, qutoed:%b]", startPosition, position, quoted);
    }
}
