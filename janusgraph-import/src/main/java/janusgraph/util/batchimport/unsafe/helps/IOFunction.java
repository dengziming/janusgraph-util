package janusgraph.util.batchimport.unsafe.helps;

import java.io.IOException;

public interface IOFunction<T, R> extends ThrowingFunction<T,R,IOException>
{
}
