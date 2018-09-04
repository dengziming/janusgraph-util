package janusgraph.util.batchimport.unsafe.input.csv;

public abstract class AnyValue
{
    private int hash;

    @Override
    public boolean equals( Object other )
    {
        return eq( other );
    }

    @Override
    public int hashCode()
    {
        //We will always recompute hashcode for values
        //where `hashCode == 0`, e.g. empty strings and empty lists
        //however that shouldn't be shouldn't be too costly
        if ( hash == 0 )
        {
            hash = computeHash();
        }
        return hash;
    }

    protected abstract boolean eq( Object other );

    protected abstract int computeHash();


    public boolean isSequenceValue()
    {
        return false; // per default Values are no SequenceValues
    }

}
