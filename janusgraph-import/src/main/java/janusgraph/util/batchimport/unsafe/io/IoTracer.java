package janusgraph.util.batchimport.unsafe.io;

/**
 * Able to retrieve stats about I/O.
 */
public interface IoTracer
{
    long countBytesWritten();

    IoTracer NONE = () -> 0;

    // TODO more IoTracer
}
