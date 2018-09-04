package janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string;


import janusgraph.util.batchimport.unsafe.idmapper.cache.ByteArray;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.helps.Numbers;

/**
 * Cache for keeping nodeId --> groupId mapping.
 */
public interface GroupCache
{
    void set(long nodeId, int groupId);

    int get(long nodeId);

    GroupCache GLOBAL = new GroupCache()
    {
        @Override
        public void set( long nodeId, int groupId )
        {   // no need
            assert groupId == Group.GLOBAL.id();
        }

        @Override
        public int get( long nodeId )
        {
            return Group.GLOBAL.id();
        }
    };

    class ByteGroupCache implements GroupCache
    {
        private final ByteArray array;

        public ByteGroupCache(NumberArrayFactory factory, int chunkSize )
        {
            array = factory.newDynamicByteArray( chunkSize, new byte[Byte.BYTES] );
        }

        @Override
        public void set( long nodeId, int groupId )
        {
            array.setByte( nodeId, 0, Numbers.safeCastIntToUnsignedByte( groupId ) );
        }

        @Override
        public int get( long nodeId )
        {
            return Numbers.unsignedByteToInt( array.getByte( nodeId, 0 ) );
        }
    }

    class ShortGroupCache implements GroupCache
    {
        private final ByteArray array;

        public ShortGroupCache( NumberArrayFactory factory, int chunkSize )
        {
            array = factory.newDynamicByteArray( chunkSize, new byte[Short.BYTES] );
        }

        @Override
        public void set( long nodeId, int groupId )
        {
            array.setShort( nodeId, 0, Numbers.safeCastIntToUnsignedShort( groupId ) );
        }

        @Override
        public int get( long nodeId )
        {
            return Numbers.unsignedShortToInt( array.getShort( nodeId, 0 ) );
        }
    }

    static GroupCache select(NumberArrayFactory factory, int chunkSize, int numberOfGroups)
    {
        if ( numberOfGroups == 0 )
        {
            return GLOBAL;
        }
        if ( numberOfGroups <= 0x100 )
        {
            return new ByteGroupCache( factory, chunkSize );
        }
        if ( numberOfGroups <= 0x10000 )
        {
            return new ShortGroupCache( factory, chunkSize );
        }
        throw new IllegalArgumentException( "Max allowed groups is " + 0xFFFF + ", but wanted " + numberOfGroups );
    }
}
