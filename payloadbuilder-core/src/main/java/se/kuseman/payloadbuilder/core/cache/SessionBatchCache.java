package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;

/** Implementation of batch cache that stores values in memory in the session */
public class SessionBatchCache extends ASessionCache<List<TupleVector>> implements BatchCache
{
    @Override
    public <TKey> Map<TKey, List<TupleVector>> getAll(QualifiedName name, Iterable<TKey> keys)
    {
        Cache<List<TupleVector>> cache = getCache(name);
        Map<TKey, List<TupleVector>> result = new HashMap<>();
        for (TKey key : keys)
        {
            CacheEntry<List<TupleVector>> entry = cache.get(key);
            result.put(key, entry != null ? entry.value
                    : null);
        }
        return result;
    }

    @Override
    public <TKey> void putAll(QualifiedName name, Map<TKey, List<TupleVector>> values, Duration ttl)
    {
        Cache<List<TupleVector>> cache = getCache(name);
        cache.putAll(values, ttl);
    }
}
