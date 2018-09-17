package janusgraph.util.batchimport.unsafe.io.fs;


import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.CopyOption;

public interface FileSystem extends Closeable
{


    OutputStream openAsOutputStream(File fileName, boolean append) throws IOException;


    boolean fileExists(File fileName);

    void mkdirs(File fileName) throws IOException;

    public boolean mkdir(File fileName);
    long getFileSize(File fileName);

    boolean deleteFile(File fileName);

    void renameFile(File from, File to, CopyOption... copyOptions) throws IOException;

    File[] listFiles(File directory);

    boolean isDirectory(File file);

    interface ThirdPartyFileSystem extends Closeable
    {
        @Override
        void close();

    }

}
