package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Implementation of custom cache that stores data in session */
public class SessionGenericCache extends ASessionCache<Object> implements GenericCache
{
    @SuppressWarnings("unchecked")
    @Override
    public <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<T> supplier)
    {
        Cache<T> entry = (Cache<T>) getCache(name);
        return entry.computeIfAbsent(key, ttl, supplier);
    }
}
