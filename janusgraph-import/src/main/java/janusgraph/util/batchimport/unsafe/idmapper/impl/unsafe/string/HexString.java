package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;

/**
 * Utility for dealing with hexadecimal strings.
 */
public class HexString
{
    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    private HexString()
    {
    }

    /**
     * Converts a byte array to a hexadecimal string.
     *
     * @param bytes Bytes to be encoded
     * @return A string of hex characters [0-9A-F]
     */
    public static String encodeHexString( byte[] bytes )
    {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ )
        {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String( hexChars );
    }

    /**
     * Converts a hexadecimal string to a byte array
     *
     * @param hexString A string of hexadecimal characters [0-9A-Fa-f] to decode
     * @return Decoded bytes, or null if the {@param hexString} is not valid
     */
    public static byte[] decodeHexString( String hexString )
    {
        int len = hexString.length();
        byte[] data = new byte[len / 2];
        for ( int i = 0, j = 0; i < len; i += 2, j++ )
        {
            int highByte = Character.digit( hexString.charAt( i ), 16 ) << 4;
            int lowByte = Character.digit( hexString.charAt( i + 1 ), 16 );
            if ( highByte < 0 || lowByte < 0 )
            {
                return null;
            }
            data[j] = (byte) ( highByte + lowByte );
        }
        return data;
    }
}
