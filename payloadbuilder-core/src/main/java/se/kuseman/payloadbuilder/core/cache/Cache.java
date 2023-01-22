package se.kuseman.payloadbuilder.core.cache;

import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Definition of a cache produced by a {@link CacheProvider} */
public interface Cache
{
    /** Return name of cache */
    QualifiedName getName();

    /** Return size of cache */
    int getSize();

    /** Return cache hits */
    int getCacheHits();

    /** Return cache stale hits */
    int getCacheStaleHits();

    /** Return cache misses */
    int getCacheMisses();

    /** Return time when cache was last accessed */
    ZonedDateTime getLastAccessTime();

    /** Return time when cache was last reloaded for any key */
    ZonedDateTime getLastReloadTime();

    /** Flush cache */
    void flush();

    /** Flush key from cache */
    void flush(Object key);

    /** Return the cache hit ratio */
    default float getCacheHitRatio()
    {
        return (float) getCacheHits() / (getCacheHits() + getCacheMisses());
    }

    /** Return the cache miss ratio */
    default float getCacheMissRatio()
    {
        return (float) getCacheMisses() / (getCacheHits() + getCacheMisses());
    }

    /**
     * Return cache entry infos for this cache
     */
    List<CacheEntry> getCacheEntries();

    /** Info about a cache entry */
    interface CacheEntry
    {
        /** Return the cache key */
        Object getKey();

        /** Return the expire time */
        ZonedDateTime getExpireTime();
    }
}