package janusgraph.util.batchimport.unsafe;



import janusgraph.util.batchimport.unsafe.input.InputChunk;
import janusgraph.util.batchimport.unsafe.input.InputIterator;
import janusgraph.util.batchimport.unsafe.output.EntityImporter;
import janusgraph.util.batchimport.unsafe.stage.StageControl;

import java.util.concurrent.atomic.LongAdder;

import static janusgraph.util.batchimport.unsafe.helps.Exceptions.launderedException;


/**
 * Allocates its own {@link InputChunk} and loops, getting input data, importing input data into store
 * until no more chunks are available.
 */
class ExhaustingEntityImporterRunnable implements Runnable
{
    private final InputIterator data;
    private final EntityImporter visitor;
    private final LongAdder roughEntityCountProgress;
    private final StageControl control;

    ExhaustingEntityImporterRunnable(StageControl control,
                                     InputIterator data, EntityImporter visitor, LongAdder roughEntityCountProgress )
    {
        this.control = control;
        this.data = data;
        this.visitor = visitor;
        this.roughEntityCountProgress = roughEntityCountProgress;
    }


    @Override
    public void run()
    {
        try ( InputChunk chunk = data.newChunk() )
        {
            while ( data.next( chunk ) )
            {
                control.assertHealthy();
                int count = 0;
                while ( chunk.next( visitor ) )
                {
                    count++;
                }
                roughEntityCountProgress.add( count );
            }
        }
        catch ( Throwable e )
        {
            control.panic( e );
            throw launderedException( e );
        }
        finally
        {
            visitor.close();
        }
    }
}
