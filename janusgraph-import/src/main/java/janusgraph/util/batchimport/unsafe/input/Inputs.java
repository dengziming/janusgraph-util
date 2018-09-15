package janusgraph.util.batchimport.unsafe.input;

import janusgraph.util.batchimport.unsafe.idmapper.IdMapper;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.input.csv.Input;
import janusgraph.util.batchimport.unsafe.input.csv.Value;

import java.util.function.ToIntFunction;

public class Inputs
{
    private Inputs()
    {
    }

    public static Input input(
            final InputIterable nodes, final InputIterable edges,
            final IdMapper idMapper, final Collector badCollector, Input.Estimates estimates )
    {
        return new Input()
        {
            @Override
            public InputIterable edges()
            {
                return edges;
            }

            @Override
            public InputIterable nodes()
            {
                return nodes;
            }

            @Override
            public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
            {
                return idMapper;
            }

            @Override
            public Collector badCollector()
            {
                return badCollector;
            }

            @Override
            public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator )
            {
                return estimates;
            }
        };
    }

    public static Input.Estimates knownEstimates(
            long numberOfNodes, long numberOfEdges,
            long numberOfNodeProperties, long numberOfEdgeProperties,
            long nodePropertiesSize, long edgePropertiesSize,
            long numberOfNodeLabels )
    {
        return new Input.Estimates()
        {
            @Override
            public long numberOfNodes()
            {
                return numberOfNodes;
            }

            @Override
            public long numberOfEdges()
            {
                return numberOfEdges;
            }

            @Override
            public long numberOfNodeProperties()
            {
                return numberOfNodeProperties;
            }

            @Override
            public long sizeOfNodeProperties()
            {
                return nodePropertiesSize;
            }

            @Override
            public long numberOfNodeLabels()
            {
                return numberOfNodeLabels;
            }

            @Override
            public long numberOfEdgeProperties()
            {
                return numberOfEdgeProperties;
            }

            @Override
            public long sizeOfEdgeProperties()
            {
                return edgePropertiesSize;
            }
        };
    }

    public static int calculatePropertySize( InputEntity entity, ToIntFunction<Value[]> valueSizeCalculator )
    {
        int size = 0;
        int propertyCount = entity.propertyCount();
        if ( propertyCount > 0 )
        {
            Value[] values = new Value[propertyCount];
            for ( int i = 0; i < propertyCount; i++ )
            {
//                values[i] = Values.of( entity.propertyValue( i ) );
                // TODO
//                values[i] =  getSize (entity.propertyValue( i )) ;
            }
            size += valueSizeCalculator.applyAsInt( values );
        }
        return size;
    }
}
