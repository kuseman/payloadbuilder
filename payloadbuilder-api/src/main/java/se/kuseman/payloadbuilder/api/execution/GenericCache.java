package se.kuseman.payloadbuilder.api.execution;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/**
 * Definition of a generic cache. Allowing for storing arbitrary data in catalogs etc.
 */
public interface GenericCache
{
    /**
     * Compute or get cached value
     *
     * @param name Name of cache
     * @param key Cache key
     * @param ttl TTL duration of cache entry
     * @param supplier Supplier that is executed when a cache miss was encountered
     */
    <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, Supplier<T> supplier);
}
