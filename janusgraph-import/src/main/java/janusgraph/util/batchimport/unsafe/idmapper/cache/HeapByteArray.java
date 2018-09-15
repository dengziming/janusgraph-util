package janusgraph.util.batchimport.unsafe.idmapper.cache;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.Math.toIntExact;

public class HeapByteArray
{

    protected static int get3ByteIntFromByteBuffer( ByteBuffer buffer, int address )
    {
        int lowWord = buffer.getShort( address ) & 0xFFFF;
        int highByte = buffer.get( address + Short.BYTES ) & 0xFF;
        int result = lowWord | (highByte << Short.SIZE);
        return result == 0xFFFFFF ? -1 : result;
    }

    protected static long get5BLongFromByteBuffer( ByteBuffer buffer, int startOffset )
    {
        long low4b = buffer.getInt( startOffset ) & 0xFFFFFFFFL;
        long high1b = buffer.get( startOffset + Integer.BYTES ) & 0xFF;
        long result = low4b | (high1b << 32);
        return result == 0xFFFFFFFFFFL ? -1 : result;
    }

    protected static long get6BLongFromByteBuffer( ByteBuffer buffer, int startOffset )
    {
        long low4b = buffer.getInt( startOffset ) & 0xFFFFFFFFL;
        long high2b = buffer.getShort( startOffset + Integer.BYTES ) & 0xFFFF;
        long result = low4b | (high2b << 32);
        return result == 0xFFFFFFFFFFFFL ? -1 : result;
    }
}
