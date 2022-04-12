package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.core.operator.TemporaryTable;

/** Implementation of temp table cache that stores data in session */
public class SessionTempTableCacheProvider extends ASessionCacheProvider<TemporaryTable> implements TempTableCacheProvider
{
    @Override
    public TemporaryTable computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<TemporaryTable> supplier)
    {
        Cache<TemporaryTable> entry = getCache(name);
        return entry.computeIfAbsent(key, ttl, supplier);
    }
}
