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
            final InputIterable nodes, final InputIterable relationships,
            final IdMapper idMapper, final Collector badCollector, Input.Estimates estimates )
    {
        return new Input()
        {
            @Override
            public InputIterable relationships()
            {
                return relationships;
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
            long numberOfNodes, long numberOfRelationships,
            long numberOfNodeProperties, long numberOfRelationshipProperties,
            long nodePropertiesSize, long relationshipPropertiesSize,
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
            public long numberOfRelationships()
            {
                return numberOfRelationships;
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
            public long numberOfRelationshipProperties()
            {
                return numberOfRelationshipProperties;
            }

            @Override
            public long sizeOfRelationshipProperties()
            {
                return relationshipPropertiesSize;
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
