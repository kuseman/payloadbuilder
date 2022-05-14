package se.kuseman.payloadbuilder.core.cache;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Cache info */
public class CacheInfo
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