package org.kuse.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import org.kuse.payloadbuilder.core.operator.TemporaryTable;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Implementation of temp table cache that stores data in session */
public class SessionTempTableCacheProvider extends ASessionCacheProvider<TemporaryTable> implements TempTableCacheProvider
{
    @Override
    public TemporaryTable computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<TemporaryTable> supplier)
    {
        CacheEntry<TemporaryTable> entry = cache.computeIfAbsent(name, k -> new CacheEntry<>());
        return entry.computeIfAbsent(key, supplier);
    }
}
