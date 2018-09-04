package janusgraph.util.batchimport.unsafe.io.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StoreFileChannel implements StoreChannel
{
    private final FileChannel channel;

    public StoreFileChannel( FileChannel channel )
    {
        this.channel = channel;
    }

    public StoreFileChannel( StoreFileChannel channel )
    {
        this.channel = channel.channel;
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return channel.write( srcs );
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return channel.write( srcs, offset, length );
    }

    @Override
    public StoreFileChannel truncate( long size ) throws IOException
    {
        channel.truncate( size );
        return this;
    }

    @Override
    public StoreFileChannel position( long newPosition ) throws IOException
    {
        channel.position( newPosition );
        return this;
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        return channel.read( dst, position );
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        channel.force( metaData );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return channel.read( dst );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return channel.read( dsts, offset, length );
    }

    @Override
    public long position() throws IOException
    {
        return channel.position();
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return channel.read( dsts );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return channel.write( src );
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public long size() throws IOException
    {
        return channel.size();
    }

    @Override
    public void flush() throws IOException
    {
        force( false );
    }

    static FileChannel unwrap( StoreChannel channel )
    {
        StoreFileChannel sfc = (StoreFileChannel) channel;
        return sfc.channel;
    }
}
