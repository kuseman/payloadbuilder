package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Implementation of generic cache that stores data in memory */
public class InMemoryGenericCache extends AInMemoryCache<Object> implements GenericCache
{
    public InMemoryGenericCache()
    {
        this(false);
    }

    public InMemoryGenericCache(boolean enableJmx)
    {
        super(CacheType.GENERIC, enableJmx);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<T> supplier)
    {
        CacheImpl<T> entry = (CacheImpl<T>) getCacheOrCreate(name);
        return entry.computeIfAbsent(key, ttl, supplier);
    }
}
