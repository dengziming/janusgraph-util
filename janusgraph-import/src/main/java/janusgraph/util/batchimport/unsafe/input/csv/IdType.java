package janusgraph.util.batchimport.unsafe.input.csv;


import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.idmapper.IdMappers;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.input.Groups;
import janusgraph.util.batchimport.unsafe.input.reader.Extractor;
import janusgraph.util.batchimport.unsafe.input.reader.Extractors;

/**
 * Defines different types that input ids can come in. Enum names in here are user facing.
 *
 * @see Header.Entry#extractor()
 */
public enum IdType
{
    /**
     * Used when node ids int input data are any string identifier.
     */
    STRING( true )
    {
        @Override
        public Extractor<?> extractor(Extractors extractors )
        {
            return extractors.string();
        }

        @Override
        public IdMapper<String> idMapper(NumberArrayFactory numberArrayFactory, Groups groups )
        {
            return IdMappers.strings( numberArrayFactory, groups );
        }
    },

    /**
     * Used when node ids int input data are any integer identifier. It uses 8b longs for storage,
     * but as a user facing enum a better name is integer
     */
    INTEGER( true )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.long_();
        }

        @Override
        public IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups )
        {
            return null;
        }
    },

    /**
     * Used when node ids int input data are specified as long values and points to actual record ids.
     * ADVANCED usage. Performance advantage, but requires carefully planned input data.
     */
    ACTUAL( false )
    {
        @Override
        public Extractor<?> extractor( Extractors extractors )
        {
            return extractors.long_();
        }

        @Override
        public IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups )
        {
            return IdMappers.actual();
        }
    };

    private final boolean idsAreExternal;

    IdType( boolean idsAreExternal )
    {
        this.idsAreExternal = idsAreExternal;
    }

    public abstract IdMapper idMapper( NumberArrayFactory numberArrayFactory, Groups groups );

    public boolean idsAreExternal()
    {
        return idsAreExternal;
    }

    public abstract Extractor<?> extractor( Extractors extractors );
}
