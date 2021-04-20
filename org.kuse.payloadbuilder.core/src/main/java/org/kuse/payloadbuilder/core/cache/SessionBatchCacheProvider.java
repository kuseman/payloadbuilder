package org.kuse.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Implementation of batch cache that stores values in memory in the session */
public class SessionBatchCacheProvider extends ASessionCacheProvider<List<Tuple>> implements BatchCacheProvider
{
    @Override
    public <TKey> Map<TKey, List<Tuple>> getAll(QualifiedName name, Iterable<TKey> keys)
    {
        CacheEntry<List<Tuple>> entry = cache.computeIfAbsent(name, k -> new CacheEntry<>());
        Map<TKey, List<Tuple>> result = new HashMap<>();
        for (TKey key : keys)
        {
            result.put(key, entry.get(key));
        }
        return result;
    }

    @Override
    public <TKey> void putAll(QualifiedName name, Map<TKey, List<Tuple>> values, Duration ttl)
    {
        CacheEntry<List<Tuple>> entry = cache.get(name);
        entry.element.putAll(values);
    }
}
