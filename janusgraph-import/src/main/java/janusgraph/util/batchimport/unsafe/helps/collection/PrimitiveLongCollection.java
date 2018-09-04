package janusgraph.util.batchimport.unsafe.helps.collection;

public interface PrimitiveLongCollection extends PrimitiveCollection, PrimitiveLongIterable
{
    /**
     * Visit the keys of this collection, until all have been visited or the visitor returns 'true'.
     */
    <E extends Exception> void visitKeys(PrimitiveLongVisitor<E> visitor) throws E;
}
