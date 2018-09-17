package janusgraph.util.batchimport.unsafe.io.fs;

import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.*;

import static java.nio.file.StandardOpenOption.*;


public class FileUtils
{
    private static final int WINDOWS_RETRY_COUNT = 5;

    private FileUtils()
    {
        throw new AssertionError();
    }

    public static boolean deleteFile( File file )
    {
        if ( !file.exists() )
        {
            return true;
        }
        int count = 0;
        boolean deleted;
        do
        {
            deleted = file.delete();
            if ( !deleted )
            {
                count++;
                waitAndThenTriggerGC();
            }
        }
        while ( !deleted && count <= WINDOWS_RETRY_COUNT );
        return deleted;
    }

    /*
     * See http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154.
     */
    private static void waitAndThenTriggerGC()
    {
        try
        {
            Thread.sleep( 500 );
        }
        catch ( InterruptedException ee )
        {
            Thread.interrupted();
        } // ok
        System.gc();
    }

    public static File path( String root, String... path )
    {
        return path( new File( root ), path );
    }

    public static File path( File root, String... path )
    {
        for ( String part : path )
        {
            root = new File( root, part );
        }
        return root;
    }

    /**
     * Attempts to discern if the given path is mounted on a device that can likely sustain a very high IO throughput.
     * <p>
     * A high IO device is expected to have negligible seek time, if any, and be able to service multiple IO requests
     * in parallel.
     *
     * @param pathOnDevice Any path, hypothetical or real, that once fully resolved, would exist on a storage device
     * that either supports high IO, or not.
     * @param defaultHunch The default hunch for whether the device supports high IO or not. This will be returned if
     * we otherwise have no clue about the nature of the storage device.
     * @return Our best-effort estimate for whether or not this device supports a high IO workload.
     */
    public static boolean highIODevice( Path pathOnDevice, boolean defaultHunch )
    {
        // This method has been manually tested and correctly identifies the high IO volumes on our test servers.
        if ( SystemUtils.IS_OS_MAC )
        {
            // Most macs have flash storage, so let's assume true for them.
            return true;
        }

        if ( SystemUtils.IS_OS_LINUX )
        {
            try
            {
                FileStore fileStore = Files.getFileStore( pathOnDevice );
                String name = fileStore.name();
                if ( name.equals( "tmpfs" ) || name.equals( "hugetlbfs" ) )
                {
                    // This is a purely in-memory device. It doesn't get faster than this.
                    return true;
                }

                if ( name.startsWith( "/dev/nvme" ) )
                {
                    // This is probably an NVMe device. Anything on that protocol is most likely very fast.
                    return true;
                }

                Path device = Paths.get( name ).toRealPath(); // Use toRealPath to resolve any symlinks.
                Path deviceName = device.getName( device.getNameCount() - 1 );

                Path rotational = rotationalPathFor( deviceName );
                if ( Files.exists( rotational ) )
                {
                    return readFirstCharacter( rotational ) == '0';
                }
                else
                {
                    String namePart = deviceName.toString();
                    int len = namePart.length();
                    while ( Character.isDigit( namePart.charAt( len - 1 ) ) )
                    {
                        len--;
                    }
                    deviceName = Paths.get( namePart.substring( 0, len ) );
                    rotational = rotationalPathFor( deviceName );
                    if ( Files.exists( rotational ) )
                    {
                        return readFirstCharacter( rotational ) == '0';
                    }
                }
            }
            catch ( Exception e )
            {
                return defaultHunch;
            }
        }

        return defaultHunch;
    }

    private static Path rotationalPathFor( Path deviceName )
    {
        return Paths.get( "/sys/block" ).resolve( deviceName ).resolve( "queue" ).resolve( "rotational" );
    }

    private static int readFirstCharacter( Path file ) throws IOException
    {
        try ( InputStream in = Files.newInputStream( file, READ ) )
        {
            return in.read();
        }
    }

    public interface LineListener
    {
        void line(String line);
    }

    public static void readTextFile( File file, LineListener listener ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) )
        {
            String line;
            while ( (line = reader.readLine()) != null )
            {
                listener.line( line );
            }
        }
    }

    public static String readTextFile( File file, Charset charset ) throws IOException
    {
        StringBuilder out = new StringBuilder();
        for ( String s : Files.readAllLines( file.toPath(), charset ) )
        {
            out.append( s ).append( "\n" );
        }
        return out.toString();
    }


    /**
     * Calculates the size of a given directory or file given the provided abstract filesystem.
     *
     * @param fs the filesystem abstraction to use
     * @param path to the file or directory.
     * @return the size, in bytes, of the file or the total size of the content in the directory, including
     * subdirectories.
     */
    public static long size(janusgraph.util.batchimport.unsafe.io.fs.FileSystem fs, File path )
    {
        if ( fs.isDirectory( path ) )
        {
            long size = 0L;
            File[] files = fs.listFiles( path );
            if ( files == null )
            {
                return 0L;
            }
            for ( File child : files )
            {
                size += size( fs, child );
            }
            return size;
        }
        else
        {
            return fs.getFileSize( path );
        }
    }
}
