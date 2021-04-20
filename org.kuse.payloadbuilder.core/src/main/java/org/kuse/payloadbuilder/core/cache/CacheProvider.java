package org.kuse.payloadbuilder.core.cache;

import static java.util.Collections.emptyList;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

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

        CacheInfo(QualifiedName name, int size, int cacheHits, int cacheMisses)
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
}
