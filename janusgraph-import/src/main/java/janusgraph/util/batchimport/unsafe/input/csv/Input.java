package janusgraph.util.batchimport.unsafe.input.csv;


import janusgraph.util.batchimport.unsafe.BatchImporter;
import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.InputIterable;
import janusgraph.util.batchimport.unsafe.input.InputIterator;

import java.io.IOException;
import java.util.function.ToIntFunction;

/**
 * Unifies all data input given to a {@link BatchImporter} to allow for more coherent implementations.
 */
public interface Input
{
    interface Estimates
    {
        /**
         * @return estimated number of nodes for the entire input.
         */
        long numberOfNodes();

        /**
         * @return estimated number of relationships for the entire input.
         */
        long numberOfRelationships();

        /**
         * @return estimated number of node properties.
         */
        long numberOfNodeProperties();

        /**
         * @return estimated number of relationship properties.
         */
        long numberOfRelationshipProperties();

        /**
         * @return estimated size that the estimated number of node properties will require on disk.
         * This is a separate estimate since it depends on the type and size of the actual properties.
         */
        long sizeOfNodeProperties();

        /**
         * @return estimated size that the estimated number of relationship properties will require on disk.
         * This is a separate estimate since it depends on the type and size of the actual properties.
         */
        long sizeOfRelationshipProperties();

        /**
         * @return estimated number of node labels. Examples:
         * <ul>
         * <li>2 nodes, 1 label each ==> 2</li>
         * <li>1 node, 2 labels each ==> 2</li>
         * <li>2 nodes, 2 labels each ==> 4</li>
         * </ul>
         */
        long numberOfNodeLabels();
    }

    /**
     * Provides all node data for an import.
     *
     * @return an {@link InputIterator} which will provide all node data for the whole import.
     */
    InputIterable nodes();

    /**
     * Provides all relationship data for an import.
     *
     * @return an {@link InputIterator} which will provide all relationship data for the whole import.
     */
    InputIterable relationships();

    /**
     * @return {@link IdMapper} which will get populated by {@link InputNode#id() input node ids}
     * and later queried by {@link InputRelationship#startNode()} and {@link InputRelationship#endNode()} ids
     * to resolve potentially temporary input node ids to actual node ids in the database.
     * @param numberArrayFactory The factory for creating data-structures to use for caching internally in the IdMapper.
     */
    IdMapper<String> idMapper(NumberArrayFactory numberArrayFactory);

    /**
     * @return a {@link Collector} capable of writing {@link InputRelationship bad relationships}
     * and {@link InputNode duplicate nodes} to an output stream for later handling.
     */
    Collector badCollector();

    /**
     * @param valueSizeCalculator for calculating property sizes on disk.
     * @return {@link Estimates} for this input w/o reading through it entirely.
     * @throws IOException on I/O error.
     */
    Estimates calculateEstimates(ToIntFunction<Value[]> valueSizeCalculator) throws Exception; // TODO
}
