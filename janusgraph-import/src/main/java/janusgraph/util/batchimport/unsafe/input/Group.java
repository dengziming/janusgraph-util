package janusgraph.util.batchimport.unsafe.input;


/**
 * Group of {@link InputEntity inputs}. Used primarily in {@link IdMapper} for supporting multiple
 * id groups within the same index.
 */
public interface Group
{
    /**
     * @return id of this group, used for identifying this group.
     */
    int id();

    /**
     * @return the name of this group.
     */
    String name();

    /**
     * @return {@link #name()}.
     */
    @Override
    String toString();

    class Adapter implements Group
    {
        private final int id;
        private final String name;

        public Adapter( int id, String name )
        {
            this.id = id;
            this.name = name;
        }

        @Override
        public int id()
        {
            return id;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return "(" + name + "," + id + ")";
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + id;
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof Group && ((Group)obj).id() == id;
        }
    }

    Group GLOBAL = new Adapter( 0, "global id space" );
}
