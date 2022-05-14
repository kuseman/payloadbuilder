package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Implementation of custom cache that stores data in session */
public class SessionGenericCache extends ASessionCache<Object> implements GenericCache
{
    @SuppressWarnings("unchecked")
    @Override
    public <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<Object> supplier)
    {
        Cache<Object> entry = getCache(name);
        return (T) entry.computeIfAbsent(key, ttl, supplier);
    }
}
