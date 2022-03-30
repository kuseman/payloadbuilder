package org.kuse.payloadbuilder.core.cache;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * Base class for session based caches. Implemented using a simple HashMap
 *
 * @param <TValue> Type of value in the cache
 */
abstract class ASessionCacheProvider<TValue> implements CacheProvider
{
    protected final Map<QualifiedName, Cache<TValue>> caches = new HashMap<>();

    @Override
    public String getName()
    {
        return "Session";
    }

    @Override
    public List<CacheInfo> getCaches()
    {
        return caches.entrySet()
                .stream()
                .map(e -> new CacheInfo(
                        e.getKey(),
                        e.getValue().elements.size(),
                        e.getValue().cacheHits,
                        e.getValue().cacheMisses))
                .collect(toList());
    }

    @Override
    public List<CacheEntryInfo> getCacheEntries(QualifiedName cacheName)
    {
        Cache<TValue> cache = caches.get(cacheName);
        if (cache == null)
        {
            return emptyList();
        }
        return cache.elements
                .entrySet()
                .stream()
                .map(e -> new CacheEntryInfo(e.getKey(),
                        e.getValue().expireTime >= 0
                            ? ZonedDateTime.ofInstant(Instant.ofEpochMilli(e.getValue().expireTime), ZoneId.systemDefault())
                            : null))
                .collect(toList());
    }

    @Override
    public void flush(QualifiedName name)
    {
        Cache<TValue> cacheEntry = caches.get(name);
        if (cacheEntry != null)
        {
            cacheEntry.elements.clear();
        }
    }

    @Override
    public void flush(QualifiedName name, Object key)
    {
        Cache<TValue> cacheEntry = caches.get(name);
        if (cacheEntry != null)
        {
            cacheEntry.elements.remove(key);
        }
    }

    @Override
    public void flushAll()
    {
        for (Cache<TValue> e : caches.values())
        {
            e.elements.clear();
        }
    }

    @Override
    public void remove(QualifiedName name)
    {
        caches.remove(name);
    }

    @Override
    public void removeAll()
    {
        caches.clear();
    }

    protected Cache<TValue> getCache(QualifiedName name)
    {
        return caches.computeIfAbsent(name, k -> new Cache<>());
    }

    /**
     * Cache
     *
     * @param <T> Type of object in cache
     */
    static class Cache<T>
    {
        final Map<Object, CacheEntry<T>> elements;
        int cacheHits;
        int cacheMisses;

        Cache()
        {
            elements = new HashMap<>();
        }

        CacheEntry<T> get(Object key)
        {
            CacheEntry<T> result = elements.get(key);
            if (result == null || result.isExpired())
            {
                cacheMisses++;
                if (result != null)
                {
                    elements.remove(key);
                }
                return null;
            }
            cacheHits++;
            return result;
        }

        <TKey> void putAll(Map<TKey, T> values, Duration ttl)
        {
            for (Entry<TKey, T> e : values.entrySet())
            {
                elements.put(e.getKey(), new CacheEntry<>(e.getValue(), ttl));
            }
        }

        T computeIfAbsent(Object key, Duration ttl, Supplier<T> supplier)
        {
            return elements.compute(key, (k, v) ->
            {
                // No mapping or expired
                if (v == null || v.isExpired())
                {
                    cacheMisses++;
                    return new CacheEntry<>(supplier.get(), ttl);
                }

                cacheHits++;
                return v;
            }).value;
        }
    }

    /**
     * Cache entry
     *
     * @param <T> Type of object in cache
     */
    static class CacheEntry<T>
    {
        long expireTime;
        T value;

        CacheEntry(T value, Duration ttl)
        {
            this.value = value;
            this.expireTime = ttl != null
                ? (System.currentTimeMillis() + ttl.toMillis())
                : -1;
        }

        boolean isExpired()
        {
            return expireTime >= 0 && System.currentTimeMillis() >= expireTime;
        }
    }
}
