package janusgraph.util.batchimport.unsafe.io.fs;

import java.io.RandomAccessFile;

/**
 * Modes describing how {@link StoreChannel} can be opened using {@link FileSystem}.
 * <br/>
 * <br/>
 * Possible values:
 * <ul>
 * <li>
 * {@link #READ}:  Open for reading only.  Invoking any of the <b>write</b>
 * methods of the resulting object will cause an {@link java.io.IOException} to be thrown.
 * </li>
 * <li>
 * {@link #READ_WRITE}: Open for reading and writing.  If the file does not already
 * exist then an attempt will be made to create it.
 * </li>
 * <li>
 * {@link #SYNC}: Open for reading and writing, as with <tt>{@link #READ_WRITE}</tt>, and also
 * require that every update to the file's content or metadata be written synchronously to the underlying storage
 * device.
 * </li>
 * <li>
 * {@link #DSYNC}:  Open for reading and writing, as with <tt>{@link #READ_WRITE}</tt>, and also
 * require that every update to the file's content be written
 * synchronously to the underlying storage device.
 * </li>
 * </ul>
 *
 * @see RandomAccessFile
 */
public enum OpenMode
{
    READ( "r" ),
    READ_WRITE( "rw" ),
    SYNC( "rws" ),
    DSYNC( "rwd" );

    private final String mode;

    OpenMode(String mode )
    {
        this.mode = mode;
    }

    public String mode()
    {
        return mode;
    }
}
