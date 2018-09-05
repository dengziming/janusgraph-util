package janusgraph.util.batchimport.unsafe.idmapper;

import janusgraph.util.batchimport.unsafe.helps.Factory;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.idmapper.impl.StringEncoder;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.*;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.raddix.Radix;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.input.Groups;

import java.util.function.LongFunction;

import static janusgraph.util.batchimport.unsafe.idmapper.IdMapper.ID_NOT_FOUND;
import static janusgraph.util.batchimport.unsafe.idmapper.IdMappers.NO_MONITOR;
import static janusgraph.util.batchimport.unsafe.progress.ProgressListener.NONE;
import static java.lang.Math.toIntExact;


public class EncodingIdMapperTest
{

    private long fails = 0;
    private long success = 0;

    //[import]285[sort]227[get]3073
    public static void main(String[] args) throws Exception {
        EncodingIdMapperTest test = new EncodingIdMapperTest(Integer.parseInt(args[0]));
        test.shouldHandleGreatAmountsOfStuff();
    }
    private static final Group GLOBAL = new Group.Adapter(1,"PHONE");



    private final int processors;
    private final Groups groups = new Groups();

    public EncodingIdMapperTest(int processors )
    {
        this.processors = processors;
    }

    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper<String> idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        LongFunction<String> inputIdLookup = String::valueOf;
        int count = 1_000_000_000;
//        int count = 100_000;

        long time1 = System.currentTimeMillis();
        // WHEN
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            idMapper.put( inputIdLookup.apply( nodeId ), GLOBAL, nodeId << (12) );
            if (nodeId % 100_000_000 == 0){
                System.out.println("+100_000_000");
            }
        }
        long time2 = System.currentTimeMillis();
        idMapper.prepare( inputIdLookup, Collector.EMPTY, NONE );

        long time3 = System.currentTimeMillis();
        // THEN
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            String id = inputIdLookup.apply( nodeId );
            if ( idMapper.get( id, GLOBAL ) >> (12) == ID_NOT_FOUND )
            {
                fail();
            }else{
                success();
            }
        }
        long time4 = System.currentTimeMillis();

        System.out.println("[import]" + (time2 - time1)/1000 + "[sort]" + (time3 - time2)/1000 + "[get]" + (time4 - time3)/ 1000);
    }

    private void success(){
        success ++;
    }
    private void fail(){
        fails ++;
    }


    private IdMapper<String> mapper(Encoder<String> encoder, Factory<Radix> radix, EncodingIdMapper.Monitor monitor )
    {
        return mapper( encoder, radix, monitor, ParallelSort.DEFAULT );
    }

    private IdMapper<String> mapper( Encoder<String> encoder, Factory<Radix> radix, EncodingIdMapper.Monitor monitor, ParallelSort.Comparator comparator )
    {
        return mapper( encoder, radix, monitor, comparator, autoDetect( encoder ) );
    }

    private IdMapper<String> mapper( Encoder<String> encoder, Factory<Radix> radix, EncodingIdMapper.Monitor monitor, ParallelSort.Comparator comparator,
            LongFunction<CollisionValues> collisionValuesFactory )
    {
        return new EncodingIdMapper( NumberArrayFactory.OFF_HEAP, encoder, radix, monitor, BigId_TRACKER_FACTORY, groups,
                collisionValuesFactory, 1_000_000, processors, comparator );
    }

    private LongFunction<CollisionValues> autoDetect( Encoder encoder )
    {
        return numberOfCollisions -> new StringCollisionValues( NumberArrayFactory.OFF_HEAP, numberOfCollisions );

    }

    private static final TrackerFactory BigId_TRACKER_FACTORY =

            ( arrayFactory, size ) ->
                    new BigIdTracker( arrayFactory.newByteArray( size, BigIdTracker.DEFAULT_VALUE ) );



}
