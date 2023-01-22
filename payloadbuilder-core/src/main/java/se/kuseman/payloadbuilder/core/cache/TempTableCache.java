package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;

/** Cache provider for temporary tables */
public interface TempTableCache extends Cache
{
    /**
     * Get temporary table by cache name and key
     *
     * @param name Name of cache
     * @param key Cache key to use
     * @param ttl TTL Duration for cache entry. NULL if no TTL
     * @param supplier Supplier that is executed if temporary table is not found in cache.
     * @return Cached temporary table
     */
    TupleVector computIfAbsent(QualifiedName name, Object key, Duration ttl, Supplier<TupleVector> supplier);
}
