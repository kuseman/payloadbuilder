package org.kuse.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * Definition of a custom cache provider. Allowing for storing arbitrary data for catalog etc.
 */
public interface CustomCacheProvider extends CacheProvider
{
    @Override
    default Type getType()
    {
        return Type.CUSTOM;
    }

    /**
     * Compute or get cached value
     *
     * @param name Name of cache
     * @param key Cache key
     * @param ttl TTL duration of cache entry
     * @param supplier Supplier that is executed when a cache miss was encountered
     */
    <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, Supplier<Object> supplier);
}
