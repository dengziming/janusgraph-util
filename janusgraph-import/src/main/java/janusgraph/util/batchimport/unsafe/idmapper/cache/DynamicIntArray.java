package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Dynamically growing {@link LongArray}. Is given a chunk size and chunks are added as higher and higher
 * items are requested.
 *
 * @see NumberArrayFactory#newDynamicIntArray(long, int)
 */
public class DynamicIntArray extends DynamicNumberArray<IntArray> implements IntArray
{
    private final int defaultValue;

    public DynamicIntArray( NumberArrayFactory factory, long chunkSize, int defaultValue )
    {
        super( factory, chunkSize, new IntArray[0] );
        this.defaultValue = defaultValue;
    }

    @Override
    public int get( long index )
    {
        IntArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get( index ) : defaultValue;
    }

    @Override
    public void set( long index, int value )
    {
        at( index ).set( index, value );
    }

    @Override
    protected IntArray addChunk( long chunkSize, long base )
    {
        return factory.newIntArray( chunkSize, defaultValue, base );
    }
}
