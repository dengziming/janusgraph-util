package janusgraph.util.batchimport.unsafe.idmapper;

/**
 * Created by dengziming on 15/08/2018.
 *
 */
public interface Encoder<T> {

    public long encode(T key);
}
