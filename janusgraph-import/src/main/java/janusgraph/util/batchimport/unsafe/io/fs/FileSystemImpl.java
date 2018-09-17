package janusgraph.util.batchimport.unsafe.io.fs;


import janusgraph.util.batchimport.unsafe.io.IOUtils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

/**
 * Default file system abstraction that creates files using the underlying file system.
 */
public class FileSystemImpl implements FileSystem
{
    static final String UNABLE_TO_CREATE_DIRECTORY_FORMAT = "Unable to create directory path [%s] for janus-import store.";



    @Override
    public OutputStream openAsOutputStream( File fileName, boolean append ) throws IOException
    {
        return new FileOutputStream( fileName, append );
    }


    @Override
    public void mkdirs( File path ) throws IOException
    {
        if ( path.exists() )
        {
            return;
        }

        path.mkdirs();

        if ( path.exists() )
        {
            return;
        }

        throw new IOException( format( UNABLE_TO_CREATE_DIRECTORY_FORMAT, path ) );
    }

    @Override
    public boolean fileExists( File fileName )
    {
        return fileName.exists();
    }

    @Override
    public boolean mkdir( File fileName )
    {
        return fileName.mkdir();
    }

    @Override
    public long getFileSize( File fileName )
    {
        return fileName.length();
    }

    @Override
    public boolean deleteFile( File fileName )
    {
        return FileUtils.deleteFile( fileName );
    }

    @Override
    public void renameFile( File from, File to, CopyOption... copyOptions ) throws IOException
    {
        Files.move( from.toPath(), to.toPath(), copyOptions );
    }

    @Override
    public File[] listFiles( File directory )
    {
        return directory.listFiles();
    }

    @Override
    public boolean isDirectory( File file )
    {
        return file.isDirectory();
    }

    private final Map<Class<? extends ThirdPartyFileSystem>, ThirdPartyFileSystem> thirdPartyFileSystems =
            new HashMap<>();


    @Override
    public void close() throws IOException
    {
        IOUtils.closeAll( thirdPartyFileSystems.values() );
    }
}
