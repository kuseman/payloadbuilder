package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Implementation of generic cache that stores data in memory */
public class InMemoryGenericCache extends AInMemoryCache<Object> implements GenericCache
{
    public InMemoryGenericCache(String name)
    {
        this(name, false);
    }

    public InMemoryGenericCache(String name, boolean enableJmx)
    {
        this(name, enableJmx, false);
    }

    public InMemoryGenericCache(String name, boolean enableJmx, boolean alwaysLoadAsync)
    {
        super(CacheType.GENERIC, name, enableJmx, alwaysLoadAsync);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<T> supplier)
    {
        CacheImpl<T> entry = (CacheImpl<T>) getCacheOrCreate(name);
        return entry.computeIfAbsent(key, ttl, supplier);
    }
}
