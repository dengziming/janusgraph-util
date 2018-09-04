package janusgraph.util.batchimport.unsafe;

import janusgraph.util.batchimport.unsafe.stage.Stage;
import janusgraph.util.batchimport.unsafe.stage.Step;
import janusgraph.util.batchimport.unsafe.helps.ByteUnit;

import static java.lang.Math.min;
import static java.lang.Math.round;


/**
 * User controlled configuration for a {@link BatchImporter}.
 */
public interface Configuration
{
    /**
     * File name in which bad entries from the import will end up. This file will be created in the
     * database directory of the imported database, i.e. <into>/bad.log.
     */
    String BAD_FILE_NAME = "bad.log";
    long MAX_PAGE_CACHE_MEMORY = ByteUnit.mebiBytes( 480 );
    int DEFAULT_MAX_MEMORY_PERCENT = 90;

    /**
     * A {@link Stage} works with batches going through one or more {@link Step steps} where one or more threads
     * process batches at each {@link Step}. This setting dictates how big the batches that are passed around are.
     */
    default int batchSize()
    {
        return 10_000;
    }

    /**
     * For statistics the average processing time is based on total processing time divided by
     * number of batches processed. A total average is probably not that interesting so this configuration
     * option specifies how many of the latest processed batches counts in the equation above.
     */
    default int movingAverageSize()
    {
        return 100;
    }

    /**
     * Rough max number of processors (CPU cores) simultaneously used in total by importer at any given time.
     * This value should be set while taking the necessary IO threads into account; the page cache and the operating
     * system will require a couple of threads between them, to handle the IO workload the importer generates.
     * Defaults to the value provided by the {@link Runtime#availableProcessors() jvm}. There's a discrete
     * number of threads that needs to be used just to get the very basics of the import working,
     * so for that reason there's no lower bound to this value.
     *   "Processor" in the context of the batch importer is different from "thread" since when discovering
     * how many processors are fully in use there's a calculation where one thread takes up 0 < fraction <= 1
     * of a processor.
     */
    default int maxNumberOfProcessors()
    {
        return allAvailableProcessors();
    }

    static int allAvailableProcessors()
    {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * @return number of relationships threshold for considering a node dense.
     */
/*    default int denseNodeThreshold()
    {
        return Integer.parseInt( dense_node_threshold.getDefaultValue() );
    }*/

    /**
     * @return amount of memory to reserve for the page cache. This should just be "enough" for it to be able
     * to sequentially read and write a couple of stores at a time. If configured too high then there will
     * be less memory available for other caches which are critical during the import. Optimal size is
     * estimated to be 100-200 MiB. The importer will figure out an optimal page size from this value,
     * with slightly bigger page size than "normal" random access use cases.
     */
    default long pageCacheMemory()
    {
        // Get the upper bound of what we can get from the default config calculation
        // We even want to limit amount of memory a bit more since we don't need very much during import
        return min( MAX_PAGE_CACHE_MEMORY, 0 );
    }

    /**
     * @return max memory to use for import cache data structures while importing.
     * This should exclude the memory acquired by this JVM. By default this returns total physical
     * memory on the machine it's running on minus the max memory of this JVM.
     * {@value #DEFAULT_MAX_MEMORY_PERCENT}% of that figure.
     * @throws UnsupportedOperationException if available memory couldn't be determined.
     */
    default long maxMemoryUsage()
    {
        return calculateMaxMemoryFromPercent( DEFAULT_MAX_MEMORY_PERCENT );
    }

    /**
     * @return whether or not to do sequential flushing of the page cache in the during stages which
     * import nodes and relationships. Having this {@code true} will reduce random I/O and make most
     * writes happen in this single background thread and will greatly benefit hardware which generally
     * benefits from single sequential writer.
     */
    default boolean sequentialBackgroundFlushing()
    {
        return true;
    }

    /**
     * Controls whether or not to write records in parallel. Multiple threads writing records in parallel
     * doesn't necessarily mean concurrent I/O because writing is separate from page cache eviction/flushing.
     */
    default boolean parallelRecordWrites()
    {
        // Defaults to true since this benefits virtually all environments
        return true;
    }

    /**
     * Controls whether or not to read records in parallel in stages where there's no record writing.
     * Enabling this may result in multiple pages being read from underlying storage concurrently.
     */
    default boolean parallelRecordReads()
    {
        // Defaults to true since this benefits most environments
        return true;
    }

    /**
     * Controls whether or not to read records in parallel in stages where there's concurrent record writing.
     * Enabling will probably increase concurrent I/O to a point which reduces performance if underlying storage
     * isn't great at concurrent I/O, especially if also {@link #parallelRecordWrites()} is enabled.
     */
    default boolean parallelRecordReadsWhenWriting()
    {
        // Defaults to false since some environments sees less performance with this enabled
        return false;
    }

    /**
     * Whether or not to allocate memory for holding the cache on heap. The first alternative is to allocate
     * off-heap, but if there's no more available memory, but there might be in the heap the importer will
     * try to allocate chunks of the cache on heap instead. This config control whether or not to allow
     * this allocation to happen on heap.
     */
    default boolean allowCacheAllocationOnHeap()
    {
        return false;
    }

    Configuration DEFAULT = new Configuration()
    {
    };


    static boolean canDetectFreeMemory()
    {
        return OsBeanUtil.getFreePhysicalMemory() != OsBeanUtil.VALUE_UNAVAILABLE;
    }

    static long calculateMaxMemoryFromPercent(int percent)
    {
        if ( percent < 1 )
        {
            throw new IllegalArgumentException( "Expected percentage to be > 0, was " + percent );
        }
        if ( percent > 100 )
        {
            throw new IllegalArgumentException( "Expected percentage to be < 100, was " + percent );
        }
        long freePhysicalMemory = OsBeanUtil.getFreePhysicalMemory();
        if ( freePhysicalMemory == OsBeanUtil.VALUE_UNAVAILABLE )
        {
            // Unable to detect amount of free memory, so rather max memory should be explicitly set
            // in order to get best performance. However let's just go with a default of 2G in this case.
            return ByteUnit.gibiBytes( 2 );
        }

        double factor = percent / 100D;
        return round( (freePhysicalMemory - Runtime.getRuntime().maxMemory()) * factor );
    }
}
