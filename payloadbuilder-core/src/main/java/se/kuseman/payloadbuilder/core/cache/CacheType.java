package se.kuseman.payloadbuilder.core.cache;

import se.kuseman.payloadbuilder.api.execution.GenericCache;

/** Cache type */
public enum CacheType
{
    TEMPTABLE,
    GENERIC;

    /**
     * Return cache type from provided provider.
     *
     * @return Type or null if no matching type could be found
     */
    public static CacheType from(CacheProvider provider)
    {
        if (provider instanceof GenericCache)
        {
            return CacheType.GENERIC;
        }
        else if (provider instanceof TempTableCache)
        {
            return CacheType.TEMPTABLE;
        }
        return null;
    }
}
