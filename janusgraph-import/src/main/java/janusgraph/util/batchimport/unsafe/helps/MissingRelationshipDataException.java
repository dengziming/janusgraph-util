package janusgraph.util.batchimport.unsafe.helps;


import janusgraph.util.batchimport.unsafe.input.csv.Type;

public class MissingRelationshipDataException extends DataException
{
    private Type fieldType;

    public MissingRelationshipDataException(Type missedField, String message )
    {
        super( message );
        this.fieldType = missedField;
    }

    public Type getFieldType()
    {
        return fieldType;
    }
}
