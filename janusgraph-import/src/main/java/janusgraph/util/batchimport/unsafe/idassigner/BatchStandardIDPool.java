// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package janusgraph.util.batchimport.unsafe.idassigner;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idassigner.IDPool;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_CAV_BITS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_CAV_TAG;

/**
 * @author dengziming (swzmdeng@163.com)
 */

public class BatchStandardIDPool implements IDPool {

    private StandardJanusGraph graph;

    private final IDAuthority idAuthority;
    private final int partition;
    private final int idNamespace;
    private final Duration renewTimeout;


    private volatile boolean closed;

    private final int uniqueIdBitWidth;
    private final int uniqueId;

    private final AtomicLong counter = new AtomicLong(1);


    public BatchStandardIDPool(StandardJanusGraph graph,IDAuthority idAuthority, int partition, int idNamespace, long idUpperBound, Duration renewTimeout, double renewBufferPercentage) {

        this.idAuthority = idAuthority;
        Preconditions.checkArgument(partition>=0);
        this.partition = partition;
        Preconditions.checkArgument(idNamespace>=0);
        this.idNamespace = idNamespace;
        Preconditions.checkArgument(!renewTimeout.isZero(), "Renew-timeout must be positive");
        this.renewTimeout = renewTimeout;

        this.graph = graph;

        //exec.allowCoreThreadTimeOut(false);
        //exec.prestartCoreThread();

        closed = false;
        uniqueIdBitWidth = graph.getConfiguration().getConfiguration().get(IDAUTHORITY_CAV_BITS);
        uniqueId = graph.getConfiguration().getConfiguration().get(IDAUTHORITY_CAV_TAG);

    }

    @Override
    public long nextID() {

        return counter.getAndIncrement() << uniqueIdBitWidth + uniqueId;
    }

    @Override
    public void close() {

        long count = counter.get();

        for (int i = 0; i < count ;){
            try {
                IDBlock idBlock = idAuthority.getIDBlock(partition, idNamespace, renewTimeout);
                i += idBlock.numIds();
            } catch (BackendException e) {
                e.printStackTrace();
            }
        }

        closed=true;

    }
}
