package janusgraph.util.batchimport.unsafe.helps;

public enum StoreFileType
{
    STORE
    {
        @Override
        public String augment( String file )
        {
            return file;
        }
    },
    ID
    {
        @Override
        public String augment( String file )
        {
            return file + ".id";
        }
    };

    public abstract String augment( String file );
}
