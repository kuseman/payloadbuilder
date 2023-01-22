package se.kuseman.payloadbuilder.core.cache;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Base definition of a cache provider */
public interface CacheProvider
{
    /** Name of the provider */
    String getName();

    /** Return cache infos for this cache provider */
    List<Cache> getCaches();

    /** Return cache with provided name */
    Cache getCache(QualifiedName name);

    /** Flush all caches in this provider */
    void flushAll();

    /** Remove all caches from this provider */
    void removeAll();

    /** Remove cache with provided name from this provider */
    void remove(QualifiedName name);
}
