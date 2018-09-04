package janusgraph.util.batchimport.unsafe.helps;

public class Numbers
{
    public static short safeCastIntToUnsignedShort( int value )
    {
        if ( (value & ~0xFFFF) != 0 )
        {
            throw new ArithmeticException( getOverflowMessage( value, "unsigned short" ) );
        }
        return (short) value;
    }

    public static byte safeCastIntToUnsignedByte( int value )
    {
        if ( (value & ~0xFF) != 0 )
        {
            throw new ArithmeticException( getOverflowMessage( value, "unsigned byte" ) );
        }
        return (byte) value;
    }

    public static int safeCastLongToInt( long value )
    {
        if ( (int) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Integer.TYPE ) );
        }
        return (int) value;
    }

    public static short safeCastLongToShort( long value )
    {
        if ( (short) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Short.TYPE ) );
        }
        return (short) value;
    }

    public static byte safeCastLongToByte( long value )
    {
        if ( (byte) value != value )
        {
            throw new ArithmeticException( getOverflowMessage( value, Byte.TYPE ) );
        }
        return (byte) value;
    }

    public static int unsignedShortToInt( short value )
    {
        return value & 0xFFFF;
    }

    public static int unsignedByteToInt( byte value )
    {
        return value & 0xFF;
    }

    private static String getOverflowMessage( long value, Class<?> clazz )
    {
        return getOverflowMessage( value, clazz.getName() );
    }

    private static String getOverflowMessage( long value, String numericType )
    {
        return "Value " + value + " is too big to be represented as " + numericType;
    }
}
