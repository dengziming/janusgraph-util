package janusgraph.util.batchimport.unsafe.idmapper.cache;

/**
 * Dynamically growing {@link LongArray}. Is given a chunk size and chunks are added as higher and higher
 * items are requested.
 *
 * @see NumberArrayFactory#newDynamicLongArray(long, long)
 */
public class DynamicLongArray extends DynamicNumberArray<LongArray> implements LongArray
{
    private final long defaultValue;

    public DynamicLongArray( NumberArrayFactory factory, long chunkSize, long defaultValue )
    {
        super( factory, chunkSize, new LongArray[0] );
        this.defaultValue = defaultValue;
    }

    @Override
    public long get( long index )
    {
        LongArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get( index ) : defaultValue;
    }

    @Override
    public void set( long index, long value )
    {
        at( index ).set( index, value );
    }

    @Override
    protected LongArray addChunk( long chunkSize, long base )
    {
        return factory.newLongArray( chunkSize, defaultValue, base );
    }
}
