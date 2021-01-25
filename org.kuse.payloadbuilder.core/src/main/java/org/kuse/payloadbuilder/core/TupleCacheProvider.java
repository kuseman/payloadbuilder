package org.kuse.payloadbuilder.core;

import static java.util.Collections.emptyMap;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Tuple;

/** Definition of a cache provider that caches tuples.
 * NOTE! Only in memory cache supported for now. {@link Tuple) needs some
 * way of serialize/deserialize functionality to be able to add distribution support */
public interface TupleCacheProvider
{
    /** Get all cached values for provided keys.
     * NOTE! All keys should yield a map entry in result whether
     * a cached value was found or not.
     * NOTE! Returned map keys should be the same instance as provided keys
     * NOTE! If key implements {@link CacheKey} then {@ink CacheKey#getKey} should be used when reading from cache
     * @param cacheName Name of cache
     * @param keys Keys to fetch
     */
    <TKey> Map<TKey, List<Tuple>> getAll(String cacheName, Iterable<TKey> keys);

    /** Put values to cache
     * @param cacheName Name of cache
     * @param values Map with values to put to cache
     * @param ttl TTL for values in cache. Null if no TTL should be applied
     **/
    <TKey> void putAll(String cacheName, Map<TKey, List<Tuple>> values, Duration ttl);

    /** Remove cache with provided name
     * @param cacheName Name of cache to remove
     **/
    default void removeCache(String cacheName)
    {
    }

    /** Flush cache with provided name
     * @param cacheName Name of cache to flush
     **/
    default void flushCache(String cacheName)
    {
    }

    /** Flush provided key from cache
     * @param cacheName Name of cache to flush
     * @param key Key to flush
     * */
    default void flushCache(String cacheName, Object key)
    {
    }

    /** Remove provided key from cache
     * @param cacheName Name of cache to remove key from
     * @param key Key to remove
     **/
    default void removeKey(String cacheName, Object key)
    {
    }

    /** Describe provider. Return a map of cache name and their properties
     * like size, hit ratio etc. */
    default Map<String, Map<String, Object>> describe()
    {
        return emptyMap();
    }

    /** Definition of a cache key. Optional interface that can be implemented on keys
     * to be able to separate that actual key from other payload fields that isn't
     * part of the key used by the cache.
     * This to be able to provide extra data to keys, later on used when data comes back from cache
     */
    interface CacheKey
    {
        Object getKey();
    }
}
