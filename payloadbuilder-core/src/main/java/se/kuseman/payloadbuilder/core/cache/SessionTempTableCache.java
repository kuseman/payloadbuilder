package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;

/** Implementation of temp table cache that stores data in session */
public class SessionTempTableCache extends ASessionCache<TupleVector> implements TempTableCache
{
    @Override
    public TupleVector computIfAbsent(QualifiedName name, Object key, Duration ttl, final Supplier<TupleVector> supplier)
    {
        Cache<TupleVector> entry = getCache(name);
        return entry.computeIfAbsent(key, ttl, supplier);
    }
}
