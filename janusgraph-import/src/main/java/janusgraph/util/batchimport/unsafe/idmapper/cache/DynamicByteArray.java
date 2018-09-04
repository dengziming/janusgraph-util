package janusgraph.util.batchimport.unsafe.idmapper.cache;

import java.nio.ByteBuffer;

import static janusgraph.util.batchimport.unsafe.idmapper.cache.HeapByteArray.get3ByteIntFromByteBuffer;
import static janusgraph.util.batchimport.unsafe.idmapper.cache.HeapByteArray.get5BLongFromByteBuffer;
import static janusgraph.util.batchimport.unsafe.idmapper.cache.HeapByteArray.get6BLongFromByteBuffer;


public class DynamicByteArray extends DynamicNumberArray<ByteArray> implements ByteArray
{
    private final byte[] defaultValue;
    private final ByteBuffer defaultValueConvenienceBuffer;

    public DynamicByteArray( NumberArrayFactory factory, long chunkSize, byte[] defaultValue )
    {
        super( factory, chunkSize, new ByteArray[0] );
        this.defaultValue = defaultValue;
        this.defaultValueConvenienceBuffer = ByteBuffer.wrap( defaultValue );
    }

    @Override
    public void swap( long fromIndex, long toIndex )
    {
        ByteArray fromArray = at( fromIndex );
        ByteArray toArray = at( toIndex );

        // Byte-wise swap
        for ( int i = 0; i < defaultValue.length; i++ )
        {
            byte intermediary = fromArray.getByte( fromIndex, i );
            fromArray.setByte( fromIndex, i, toArray.getByte( toIndex, i ) );
            toArray.setByte( toIndex, i, intermediary );
        }
    }

    @Override
    public void get( long index, byte[] into )
    {
        ByteArray chunk = chunkOrNullAt( index );
        if ( chunk != null )
        {
            chunk.get( index, into );
        }
        else
        {
            System.arraycopy( defaultValue, 0, into, 0, defaultValue.length );
        }
    }

    @Override
    public byte getByte( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getByte( index, offset ) : defaultValueConvenienceBuffer.get( offset );
    }

    @Override
    public short getShort( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getShort( index, offset ) : defaultValueConvenienceBuffer.getShort( offset );
    }

    @Override
    public int getInt( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getInt( index, offset ) : defaultValueConvenienceBuffer.getInt( offset );
    }

    @Override
    public int get3ByteInt( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get3ByteInt( index, offset ) :
               get3ByteIntFromByteBuffer( defaultValueConvenienceBuffer, offset );
    }

    @Override
    public long get5ByteLong( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get5ByteLong( index, offset ) :
            get5BLongFromByteBuffer( defaultValueConvenienceBuffer, offset );
    }

    @Override
    public long get6ByteLong( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get6ByteLong( index, offset ) :
            get6BLongFromByteBuffer( defaultValueConvenienceBuffer, offset );
    }

    @Override
    public long getLong( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getLong( index, offset ) : defaultValueConvenienceBuffer.getLong( offset );
    }

    @Override
    public void set( long index, byte[] value )
    {
        at( index ).set( index, value );
    }

    @Override
    public void setByte( long index, int offset, byte value )
    {
        at( index ).setByte( index, offset, value );
    }

    @Override
    public void setShort( long index, int offset, short value )
    {
        at( index ).setShort( index, offset, value );
    }

    @Override
    public void setInt( long index, int offset, int value )
    {
        at( index ).setInt( index, offset, value );
    }

    @Override
    public void set5ByteLong( long index, int offset, long value )
    {
        at( index ).set5ByteLong( index, offset, value );
    }

    @Override
    public void set6ByteLong( long index, int offset, long value )
    {
        at( index ).set6ByteLong( index, offset, value );
    }

    @Override
    public void setLong( long index, int offset, long value )
    {
        at( index ).setLong( index, offset, value );
    }

    @Override
    public void set3ByteInt( long index, int offset, int value )
    {
        at( index ).set3ByteInt( index, offset, value );
    }

    @Override
    protected ByteArray addChunk( long chunkSize, long base )
    {
        return factory.newByteArray( chunkSize, defaultValue, base );
    }
}
