package janusgraph.util.batchimport.unsafe.idmapper;

import janusgraph.util.batchimport.unsafe.helps.collection.PrimitiveLongIterator;
import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.progress.ProgressListener;

import java.util.function.LongFunction;

/**
 * @author dengziming (swzmdeng@163.com,dengziming1993@gmail.com)
 */
public interface IdMapper<T> extends MemoryStatsVisitor.Visitable{
    long ID_NOT_FOUND = -1;

    public void put(T key, Group group, long id);

    public long get(T key, Group group);

    /**
     * @return whether or not a call to {@link #prepare(LongFunction, Collector, ProgressListener)} needs to commence after all calls to
     * {@link #put(Object, Group, long)} and before any call to {@link #get(Object, Group)}. I.e. whether or not all ids
     * needs to be put before making any call to {@link #get(Object, Group)}.
     */
    boolean needsPreparation();

    /**
     * After all mappings have been {@link #put(Object, Group, long)} call this method to prepare for
     * {@link #get(Object, Group)}.
     *
     * @param inputIdLookup can return input id of supplied node id. Used in the event of difficult collisions
     * so that more information have to be read from the input data again, data that normally isn't necessary
     * and hence discarded.
     * @param collector {@link Collector} for bad entries, such as duplicate node ids.
     * @param progress reports preparation progress.
     */
    void prepare(LongFunction<Object> inputIdLookup, Collector collector, ProgressListener progress) throws Exception;

    public void close();

    PrimitiveLongIterator leftOverDuplicateNodesIds();

    long calculateMemoryUsage(long numberOfNodes);
}
