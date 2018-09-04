/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package janusgraph.util.batchimport.unsafe.idmapper;

import janusgraph.util.batchimport.unsafe.helps.Factory;
import janusgraph.util.batchimport.unsafe.idmapper.cache.NumberArrayFactory;
import janusgraph.util.batchimport.unsafe.idmapper.impl.StringEncoder;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.*;
import janusgraph.util.batchimport.unsafe.idmapper.impl.unsafe.string.raddix.Radix;
import janusgraph.util.batchimport.unsafe.input.Collector;
import janusgraph.util.batchimport.unsafe.input.Group;
import janusgraph.util.batchimport.unsafe.input.Groups;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.LongFunction;

import static janusgraph.util.batchimport.unsafe.idmapper.IdMappers.NO_MONITOR;
import static janusgraph.util.batchimport.unsafe.idmapper.IdMapper.ID_NOT_FOUND;
import static janusgraph.util.batchimport.unsafe.progress.ProgressListener.NONE;
import static java.lang.Math.toIntExact;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith( Parameterized.class )
public class EncodingIdMapperTest
{
    private static final Group GLOBAL = new Group.Adapter(1,"PHONE");

    @Parameters( name = "processors:{0}" )
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( new Object[]{1} );
        data.add( new Object[]{2} );
        int bySystem = Runtime.getRuntime().availableProcessors() - 1;
        if ( bySystem > 2 )
        {
            data.add( new Object[]{bySystem} );
        }
        return data;
    }

    private final int processors;
    private final Groups groups = new Groups();

    public EncodingIdMapperTest(int processors )
    {
        this.processors = processors;
    }

    @Test
    public void shouldHandleGreatAmountsOfStuff() throws Exception
    {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        LongFunction<Object> inputIdLookup = String::valueOf;
        int count = 3_000_000;
//        int count = 100_000;
        // WHEN
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            idMapper.put( inputIdLookup.apply( nodeId ), GLOBAL, nodeId << (12) );
            if (nodeId % 100_000 == 0){
                System.out.println("+100_000");
            }
        }
        idMapper.prepare( inputIdLookup, mock( Collector.class ), NONE );

        // THEN
        for ( long nodeId = 0; nodeId < count; nodeId++ )
        {
            // the UUIDs here will be generated in the same sequence as above because we reset the random
            Object id = inputIdLookup.apply( nodeId );
            if ( idMapper.get( id, GLOBAL ) >> (12) == ID_NOT_FOUND )
            {
                fail( "Couldn't find " + id + " even though I added it just previously" );
            }else{
                success();
            }
        }
    }

    private void success(){
    }

    @Test
    public void testGetPhone() throws Exception {
        // GIVEN
        IdMapper idMapper = mapper( new StringEncoder(), Radix.STRING, NO_MONITOR );
        LongFunction<Object> inputIdLookup = String::valueOf;



        idMapper.put("开元币",GLOBAL,1 << 12);
        idMapper.put("00002986682",GLOBAL,2 << 12);
        idMapper.put("00002986683",GLOBAL,3 << 12);
        idMapper.put("00002986684",GLOBAL,4 << 12);
        idMapper.put("00002986685",GLOBAL,5 << 12);

        idMapper.prepare( inputIdLookup, mock( Collector.class ), NONE );

        System.out.println(idMapper.get("开元币",GLOBAL));
        System.out.println(idMapper.get("00002986682",GLOBAL));
        System.out.println(idMapper.get("00002986683",GLOBAL));
        System.out.println(idMapper.get("00002986684",GLOBAL));
        System.out.println(idMapper.get("00002986685",GLOBAL));
    }

    private LongFunction<Object> values( Object... values )
    {
        return value -> values[toIntExact( value )];
    }

    private IdMapper mapper(Encoder encoder, Factory<Radix> radix, EncodingIdMapper.Monitor monitor )
    {
        return mapper( encoder, radix, monitor, ParallelSort.DEFAULT );
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, EncodingIdMapper.Monitor monitor, ParallelSort.Comparator comparator )
    {
        return mapper( encoder, radix, monitor, comparator, autoDetect( encoder ) );
    }

    private IdMapper mapper( Encoder encoder, Factory<Radix> radix, EncodingIdMapper.Monitor monitor, ParallelSort.Comparator comparator,
            LongFunction<CollisionValues> collisionValuesFactory )
    {
        return new EncodingIdMapper( NumberArrayFactory.OFF_HEAP, encoder, radix, monitor, RANDOM_TRACKER_FACTORY, groups,
                collisionValuesFactory, 1_000, processors, comparator );
    }

    private LongFunction<CollisionValues> autoDetect( Encoder encoder )
    {
        return numberOfCollisions -> new StringCollisionValues( NumberArrayFactory.OFF_HEAP, numberOfCollisions );

    }

    private static final TrackerFactory RANDOM_TRACKER_FACTORY =
            ( arrayFactory, size ) -> System.currentTimeMillis() % 2 == 0
                    ? new IntTracker( arrayFactory.newIntArray( size, IntTracker.DEFAULT_VALUE ) )
                    : new BigIdTracker( arrayFactory.newByteArray( size, BigIdTracker.DEFAULT_VALUE ) );


}
