package janusgraph.util.batchimport.unsafe.helps.collection;


import janusgraph.util.batchimport.unsafe.helps.ThrowingFunction;

class RawMapIterator<FROM, TO, EX extends Exception> implements RawIterator<TO,EX>
{
    private final RawIterator<FROM,EX> fromIterator;
    private final ThrowingFunction<? super FROM,? extends TO,EX> function;

    RawMapIterator(RawIterator<FROM,EX> fromIterator, ThrowingFunction<? super FROM,? extends TO,EX> function )
    {
        this.fromIterator = fromIterator;
        this.function = function;
    }

    @Override
    public boolean hasNext() throws EX
    {
        return fromIterator.hasNext();
    }

    @Override
    public TO next() throws EX
    {
        FROM from = fromIterator.next();
        return function.apply( from );
    }

    @Override
    public void remove()
    {
        fromIterator.remove();
    }
}
