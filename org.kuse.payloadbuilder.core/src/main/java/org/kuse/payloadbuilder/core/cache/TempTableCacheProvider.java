package org.kuse.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import org.kuse.payloadbuilder.core.operator.TemporaryTable;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Cache provider for temporary tables */
public interface TempTableCacheProvider extends CacheProvider
{
    @Override
    default Type getType()
    {
        return Type.TEMPTABLE;
    }

    /**
     * Get temporary table by cache name and key
     *
     * @param name Name of cache
     * @param key Cache key to use
     * @param ttl TTL Duration for cache entry. NULL if no TTL
     * @param supplier Supplier that is executed if temporary table is not found in cache.
     * @return Cached temporary table
     */
    TemporaryTable computIfAbsent(QualifiedName name, Object key, Duration ttl, Supplier<TemporaryTable> supplier);
}
