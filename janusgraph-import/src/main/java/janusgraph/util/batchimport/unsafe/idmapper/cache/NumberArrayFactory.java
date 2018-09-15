package janusgraph.util.batchimport.unsafe.idmapper.cache;


/**
 * Factory of {@link LongArray}, {@link IntArray} and {@link ByteArray} instances. Users can select in which type of
 * memory the arrays will be placed, either in {HEAP}, {@link #OFF_HEAP}, or use an auto allocator which
 * will have each instance placed where it fits best, favoring the primary candidates.
 */
public interface NumberArrayFactory
{

    /**
     * Puts arrays off-heap, using unsafe calls.
     */
    NumberArrayFactory OFF_HEAP = new Adapter()
    {
        @Override
        public IntArray newIntArray( long length, int defaultValue, long base )
        {
            return new OffHeapIntArray( length, defaultValue, base );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base )
        {
            return new OffHeapLongArray( length, defaultValue, base );
        }

        @Override
        public ByteArray newByteArray( long length, byte[] defaultValue, long base )
        {
            return new OffHeapByteArray( length, defaultValue, base );
        }

        @Override
        public String toString()
        {
            return "OFF_HEAP";
        }
    };


    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link IntArray}.
     */
    default IntArray newIntArray(long length, int defaultValue)
    {
        return newIntArray( length, defaultValue, 0 );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @return a fixed size {@link IntArray}.
     */
    IntArray newIntArray(long length, int defaultValue, long base);

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link IntArray}.
     */
    IntArray newDynamicIntArray(long chunkSize, int defaultValue);

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link LongArray}.
     */
    default LongArray newLongArray(long length, long defaultValue)
    {
        return newLongArray( length, defaultValue, 0 );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @return a fixed size {@link LongArray}.
     */
    LongArray newLongArray(long length, long defaultValue, long base);

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link LongArray}.
     */
    LongArray newDynamicLongArray(long chunkSize, long defaultValue);

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link ByteArray}.
     */
    default ByteArray newByteArray(long length, byte[] defaultValue)
    {
        return newByteArray( length, defaultValue, 0 );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @return a fixed size {@link ByteArray}.
     */
    ByteArray newByteArray(long length, byte[] defaultValue, long base);

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link ByteArray}.
     */
    ByteArray newDynamicByteArray(long chunkSize, byte[] defaultValue);

    /**
     * Implements the dynamic array methods, because they are the same in most implementations.
     */

    abstract class Adapter implements NumberArrayFactory
    {

        @Override
        public IntArray newDynamicIntArray( long chunkSize, int defaultValue )
        {
            return new DynamicIntArray( this, chunkSize, defaultValue );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize, long defaultValue )
        {
            return new DynamicLongArray( this, chunkSize, defaultValue );
        }

        @Override
        public ByteArray newDynamicByteArray( long chunkSize, byte[] defaultValue )
        {
            return new DynamicByteArray( this, chunkSize, defaultValue );
        }

    }
}