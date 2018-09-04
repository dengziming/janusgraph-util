package janusgraph.util.batchimport.unsafe.io;


import janusgraph.util.batchimport.unsafe.io.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class consists exclusively of static methods that operate on files, directories, or other types of files.
 */
public class Files
{
    private Files()
    {
    }

    /**
     * Creates a file, or opens an existing file. If necessary, parent directories will be created.
     *
     * @param fileSystem The filesystem abstraction to use
     * @param file The file to create or open
     * @return An output stream
     * @throws IOException If an error occurs creating directories or opening the file
     */
    public static OutputStream createOrOpenAsOuputStream(FileSystem fileSystem, File file, boolean append ) throws IOException
    {
        if ( file.getParentFile() != null )
        {
            fileSystem.mkdirs( file.getParentFile() );
        }
        return fileSystem.openAsOutputStream( file, append );
    }
}
