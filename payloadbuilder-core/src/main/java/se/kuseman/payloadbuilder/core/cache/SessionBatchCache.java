package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Implementation of batch cache that stores values in memory in the session */
public class SessionBatchCache extends ASessionCache<List<Tuple>> implements BatchCache
{
    @Override
    public <TKey> Map<TKey, List<Tuple>> getAll(QualifiedName name, Iterable<TKey> keys)
    {
        Cache<List<Tuple>> cache = getCache(name);
        Map<TKey, List<Tuple>> result = new HashMap<>();
        for (TKey key : keys)
        {
            CacheEntry<List<Tuple>> entry = cache.get(key);
            if (entry != null)
            {
                result.put(key, entry.value);
            }
        }
        return result;
    }

    @Override
    public <TKey> void putAll(QualifiedName name, Map<TKey, List<Tuple>> values, Duration ttl)
    {
        Cache<List<Tuple>> cache = getCache(name);
        cache.putAll(values, ttl);
    }
}
