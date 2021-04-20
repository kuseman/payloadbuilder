package org.kuse.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Implementation of temp table cache that stores data in session */
public class SessionCustomCacheProvider extends ASessionCacheProvider<Object> implements CustomCacheProvider
{
    @SuppressWarnings("unchecked")
    @Override
    public <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<Object> supplier)
    {
        CacheEntry<Object> entry = cache.computeIfAbsent(name, k -> new CacheEntry<>());
        return (T) entry.computeIfAbsent(key, supplier);
    }
}
