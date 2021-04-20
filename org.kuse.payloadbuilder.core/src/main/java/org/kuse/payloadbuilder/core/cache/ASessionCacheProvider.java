package org.kuse.payloadbuilder.core.cache;

import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * Base class for session based caches. Implemented using a simple HashMap
 *
 * @param <TValue> Type of value in the cache
 */
abstract class ASessionCacheProvider<TValue> implements CacheProvider
{
    protected final Map<QualifiedName, CacheEntry<TValue>> cache = new HashMap<>();

    @Override
    public String getName()
    {
        return "Session";
    }

    @Override
    public List<CacheInfo> getCaches()
    {
        return cache.entrySet()
                .stream()
                .map(e -> new CacheInfo(
                        e.getKey(),
                        e.getValue().element.size(),
                        e.getValue().cacheHits,
                        e.getValue().cacheMisses))
                .collect(toList());
    }

    @Override
    public void flush(QualifiedName name)
    {
        CacheEntry<TValue> cacheEntry = cache.get(name);
        if (cacheEntry != null)
        {
            cacheEntry.element.clear();
        }
    }

    @Override
    public void flush(QualifiedName name, Object key)
    {
        CacheEntry<TValue> cacheEntry = cache.get(name);
        if (cacheEntry != null)
        {
            cacheEntry.element.remove(key);
        }
    }

    @Override
    public void flushAll()
    {
        for (CacheEntry<TValue> e : cache.values())
        {
            e.element.clear();
        }
    }

    @Override
    public void remove(QualifiedName name)
    {
        cache.remove(name);
    }

    @Override
    public void removeAll()
    {
        cache.clear();
    }

    /**
     * Cache entry
     *
     * @param <T> Type of object in cache
     */
    static class CacheEntry<T>
    {
        final Map<Object, T> element;
        int cacheHits;
        int cacheMisses;

        CacheEntry()
        {
            this.element = new HashMap<>();
        }

        T get(Object key)
        {
            T result = element.get(key);
            if (result == null)
            {
                cacheMisses++;
            }
            else
            {
                cacheHits++;
            }
            return result;
        }

        T computeIfAbsent(Object key, Supplier<T> supplier)
        {
            return element.compute(key, (k, v) ->
            {
                if (v == null)
                {
                    cacheMisses++;
                    return supplier.get();
                }
                cacheHits++;
                return v;
            });
        }
    }
}
