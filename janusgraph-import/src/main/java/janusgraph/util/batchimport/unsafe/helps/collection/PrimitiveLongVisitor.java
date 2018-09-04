package janusgraph.util.batchimport.unsafe.helps.collection;

public interface PrimitiveLongVisitor<E extends Exception>
{
    /**
     * Visit the given entry.
     *
     * @param value A distinct value from the set.
     * @return 'true' to signal that the iteration should be stopped, 'false' to signal that the iteration should
     * continue if there are more entries to look at.
     * @throws E any thrown exception of type 'E' will bubble up through the 'visit' method.
     */
    boolean visited(long value) throws E;

    PrimitiveLongVisitor<RuntimeException> EMPTY = value -> false;
}
