package janusgraph.util.batchimport.unsafe.idmapper.impl.heap;

import janusgraph.util.batchimport.unsafe.helps.collection.PrimitiveLongIterator;
import janusgraph.util.batchimport.unsafe.idmapper.cache.MemoryStatsVisitor;
import janusgraph.util.batchimport.unsafe.idmapper.impl.AbstractIdMapper;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.progress.ProgressListener;

import java.util.HashMap;
import java.util.Map;
import java.util.function.LongFunction;

/**
 * head mapper
 */
public class HeapIdMapper<T> extends AbstractIdMapper<T> {

    private Map<Group, Map<T,Long>> idContainer ;

    public HeapIdMapper(){
        idContainer = new HashMap<>();
    }

    @Override
    public void put(T key, Group group, long id) {

        if (idContainer.get(group) == null){
            Map<T, Long> groupMap = new HashMap<>();
            idContainer.put(group,groupMap);
        }
        idContainer.get(group).put(key,id);
    }

    @Override
    public long get(T key,Group group) {
        return idContainer.get(group).get(key);
    }

    @Override
    public boolean needsPreparation() {
        return false;
    }

    @Override
    public void prepare(LongFunction<T> inputIdLookup, Collector collector, ProgressListener progress) {
        // ignore
    }

    @Override
    public void close() {

    }

    @Override
    public PrimitiveLongIterator leftOverDuplicateNodesIds() {
        return null;
    }

    @Override
    public long calculateMemoryUsage(long numberOfNodes) {
        return 0;
    }

    @Override
    public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {
        // ignore
    }
}
