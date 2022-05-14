package se.kuseman.payloadbuilder.core.cache;

/** Cache type */
public enum CacheType
{
    BATCH,
    TEMPTABLE,
    CUSTOM;

    /**
     * Return cache type from provided provider.
     * 
     * @return Type or null if no matching type could be found
     */
    public static CacheType from(Cache provider)
    {
        if (provider instanceof GenericCache)
        {
            return CacheType.CUSTOM;
        }
        else if (provider instanceof BatchCache)
        {
            return CacheType.BATCH;
        }
        else if (provider instanceof TempTableCache)
        {
            return CacheType.TEMPTABLE;
        }
        return null;
    }
}
