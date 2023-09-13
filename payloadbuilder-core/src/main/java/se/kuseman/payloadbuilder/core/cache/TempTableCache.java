package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Cache provider for temporary tables */
public interface TempTableCache extends CacheProvider
{
    /**
     * Compute temporary table by cache name, ttl and supplier
     *
     * @param name Name of cache
     * @param ttl TTL Duration for cache entry. NULL if no TTL
     * @param supplier Supplier that is executed if temporary table is not found in cache.
     * @return Cached temporary table
     */
    TemporaryTable computIfAbsent(QualifiedName name, Duration ttl, Supplier<TemporaryTable> supplier);
}
