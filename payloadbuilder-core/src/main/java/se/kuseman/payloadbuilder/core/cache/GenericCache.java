package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/**
 * Definition of a custom cache provider. Allowing for storing arbitrary data for catalog etc.
 */
public interface GenericCache extends Cache, se.kuseman.payloadbuilder.api.execution.GenericCache
{
    /**
     * Compute or get cached value
     *
     * @param name Name of cache
     * @param key Cache key
     * @param ttl TTL duration of cache entry
     * @param supplier Supplier that is executed when a cache miss was encountered
     */
    @Override
    <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, Supplier<T> supplier);
}
