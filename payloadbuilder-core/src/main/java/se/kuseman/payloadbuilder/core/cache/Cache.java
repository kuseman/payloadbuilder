package se.kuseman.payloadbuilder.core.cache;

import static java.util.Collections.emptyList;

import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Base definition of a cache */
public interface Cache
{
    /** Name of the provider */
    String getName();

    /** Return cache infos for this cache provider */
    default List<CacheInfo> getCaches()
    {
        return emptyList();
    }

    /**
     * Return cache entry infos for this cache provider
     *
     * @param cacheName Name of cache to return entry infos for
     */
    default List<CacheEntryInfo> getCacheEntries(QualifiedName cacheName)
    {
        return emptyList();
    }

    /** Flush all caches in this provider */
    void flushAll();

    /** Flush cache with provided name */
    void flush(QualifiedName name);

    /** Flush key from cache with provided name */
    void flush(QualifiedName name, Object key);

    /** Remove all caches from this provider */
    void removeAll();

    /** Remove cache with provided name from this provider */
    void remove(QualifiedName name);

    /** Info about a cache entry */
    class CacheEntryInfo
    {
        private final Object key;
        private final ZonedDateTime expireTime;

        public CacheEntryInfo(Object key, ZonedDateTime expireTime)
        {
            this.key = key;
            this.expireTime = expireTime;
        }

        public Object getKey()
        {
            return key;
        }

        public ZonedDateTime getExpireTime()
        {
            return expireTime;
        }
    }
}
