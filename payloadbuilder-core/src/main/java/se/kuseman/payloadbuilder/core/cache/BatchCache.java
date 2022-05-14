package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Cache provider for the {@link se.kuseman.payloadbuilder.core.operator.BatchCacheOperator} */
public interface BatchCache extends Cache
{
    /**
     * Get all cached values for provided iterable with keys
     *
     * @param name Name of the cache
     * @param keys Keys to fetch from cache
     * @return
     *
     * <pre>
     * A map with unique keys and their associated list of tuples
     * NOTE! all input keys must provide a value in resulting map, NULL as indicator that value did not exist
     * </pre>
     **/
    <TKey> Map<TKey, List<Tuple>> getAll(QualifiedName name, Iterable<TKey> keys);

    /**
     * Put all provided values to cache
     *
     * @param name Name of the cache
     * @param values Values to put to cache
     * @param ttl TTL of the values. Null if no TTL
     */
    <TKey> void putAll(QualifiedName name, Map<TKey, List<Tuple>> values, Duration ttl);
}
