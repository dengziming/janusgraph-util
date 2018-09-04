package janusgraph.util.batchimport.unsafe.stage;

public interface Panicable
{
    /**
     * Receives a panic, asking to shut down as soon as possible.
     * @param cause cause for the panic.
     */
    void receivePanic(Throwable cause);
}
