package org.kuse.payloadbuilder.core;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Tuple;

/** Definition of a cache provider. Used by operators that supports caching
 * to cache/retrieve tuples.
 * NOTE! Only in memory cache supported for now. {@link Tuple) needs some
 * way of serialize/deserialize functionality to be able to add distribute support */
public interface CacheProvider
{
    /** Get all cached values for provided keys.
     * NOTE! All keys should yield a map entry in result whether
     * a cached value was found or not.
     * NOTE! Returned map keys should be the same instance as provided keys
     * NOTE! If key implements {@link CacheKey} then {@ink CacheKey#getKey} should be used when reading from cache
     * */
    <TKey> Map<TKey, List<Tuple>> getAll(Iterable<TKey> keys);

    /** Put values to cache
     * @param values Map with values to put to cache
     * @param ttl TTL for values in cache. Null if no TTL should be applied
     **/
    <TKey> void putAll(Map<TKey, List<Tuple>> values, Duration ttl);

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
