package janusgraph.util.batchimport.unsafe.input.csv;


import janusgraph.util.batchimport.unsafe.helps.collection.RawIterator;
import janusgraph.util.batchimport.unsafe.input.reader.CharReadable;
import janusgraph.util.batchimport.unsafe.input.reader.CharSeeker;

import java.io.IOException;

/**
 * Produces a {@link CharSeeker} that can seek and extract values from a csv/tsv style data stream.
 * A decorator also comes with it which can specify global overrides/defaults of extracted input entities.
 */
public interface Data
{
    RawIterator<CharReadable,IOException> stream();

    Decorator decorator();
}
