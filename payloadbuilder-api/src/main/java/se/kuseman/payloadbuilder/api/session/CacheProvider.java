package se.kuseman.payloadbuilder.api.session;

import static java.util.Collections.emptyList;

import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Base for cache provider */
public interface CacheProvider
{
    /** Name of the provider */
    String getName();

    /** Get type of provider */
    Type getType();

    /** Name of the custom cache type */
    default String getCustomType()
    {
        return null;
    }

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

    /** Cache type */
    enum Type
    {
        BATCH,
        TEMPTABLE,
        CUSTOM
    }

    /** Cache info */
    class CacheInfo
    {
        private final QualifiedName name;
        private final int size;
        private final int cacheHits;
        private final int cacheMisses;

        public CacheInfo(QualifiedName name, int size, int cacheHits, int cacheMisses)
        {
            this.name = name;
            this.size = size;
            this.cacheHits = cacheHits;
            this.cacheMisses = cacheMisses;
        }

        public QualifiedName getName()
        {
            return name;
        }

        public int getSize()
        {
            return size;
        }

        public int getCacheHits()
        {
            return cacheHits;
        }

        public int getCacheMisses()
        {
            return cacheMisses;
        }
    }

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
