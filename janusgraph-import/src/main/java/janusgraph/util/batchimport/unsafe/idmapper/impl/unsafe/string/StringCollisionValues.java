package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.ByteArray;
import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;

import static java.lang.Integer.min;
import static java.lang.Long.max;

/**
 * Stores {@link String strings} in a {@link ByteArray} provided by {@link NumberArrayFactory}. Each string can have different
 * length, where maximum string length is 2^16 - 1.
 */
public class StringCollisionValues implements CollisionValues
{
    private final long chunkSize;
    private final ByteArray cache;
    private long offset;
    private ByteArray current;

    public StringCollisionValues(NumberArrayFactory factory, long length )
    {
        chunkSize = max( length, 10_000 );
        cache = factory.newDynamicByteArray( chunkSize, new byte[1] );
        current = cache.at( 0 );
    }

    @Override
    public long add( Object id )
    {
        String string = (String) id;
        byte[] bytes = UTF8.encode( string );
        int length = bytes.length;
        if ( length > 0xFFFF )
        {
            throw new IllegalArgumentException( string );
        }

        if ( offset % chunkSize >= chunkSize - 2 )
        {
            // There isn't enough space left in the current chunk to begin writing this value, move over to the next one
            offset += chunkSize - (offset % chunkSize);
            current = cache.at( offset );
        }

        long startOffset = offset;
        current.setShort( offset, 0, (short) length );
        offset += 2;
        for ( int i = 0; i < length; )
        {
            int bytesLeftToWrite = length - i;
            int bytesLeftInChunk = (int) (chunkSize - offset % chunkSize);
            int bytesToWriteInThisChunk = min( bytesLeftToWrite, bytesLeftInChunk );
            for ( int j = 0; j < bytesToWriteInThisChunk; j++ )
            {
                current.setByte( offset++, 0, bytes[i++] );
            }

            if ( length > i )
            {
                current = cache.at( offset );
            }
        }

        return startOffset;
    }

    @Override
    public Object get( long offset )
    {
        ByteArray array = cache.at( offset );
        int length = array.getShort( offset, 0 ) & 0xFFFF;
        offset += 2;
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; )
        {
            int bytesLeftToRead = length - i;
            int bytesLeftInChunk = (int) (chunkSize - offset % chunkSize);
            int bytesToReadInThisChunk = min( bytesLeftToRead, bytesLeftInChunk );
            for ( int j = 0; j < bytesToReadInThisChunk; j++ )
            {
                bytes[i++] = array.getByte( offset++, 0 );
            }

            if ( length > i )
            {
                array = cache.at( offset );
            }
        }
        return UTF8.decode( bytes );
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        cache.acceptMemoryStatsVisitor( visitor );
    }

    @Override
    public void close()
    {
        cache.close();
    }
}
