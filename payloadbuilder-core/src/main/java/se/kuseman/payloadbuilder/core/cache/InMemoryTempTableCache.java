package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Implementation of temp table cache that stores data in memory. */
public class InMemoryTempTableCache extends AInMemoryCache<TemporaryTable> implements TempTableCache
{
    public InMemoryTempTableCache()
    {
        this(false);
    }

    public InMemoryTempTableCache(boolean enableJmx)
    {
        super(CacheType.TEMPTABLE, enableJmx);
    }

    @Override
    public TemporaryTable computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<TemporaryTable> supplier)
    {
        CacheImpl<TemporaryTable> entry = getCacheOrCreate(name);
        return entry.computeIfAbsent(key, ttl, supplier);
    }
}
