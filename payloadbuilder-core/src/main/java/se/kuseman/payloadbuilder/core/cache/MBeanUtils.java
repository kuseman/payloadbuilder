package se.kuseman.payloadbuilder.core.cache;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.ObjectName;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Utils for working with cache and MBeans */
public class MBeanUtils
{
    /** Register cache to mbean server */
    public static void registerCacheMBean(CacheType type, QualifiedName name, Cache cache)
    {
        try
        {
            ObjectName objectName = ObjectName.getInstance(String.format("se.kuseman.payloadbuilder:type=Cache,CacheType=%s,Name=%s", type.name()
                    .toLowerCase(), name.toDotDelimited()));
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(new CacheMBeanImpl(cache), objectName);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cannot register cache MBean", e);
        }
    }

    /** Definition of a cache mbean */
    @MXBean
    public interface CacheMBean
    {
        int getSize();

        int getCacheHits();

        int getCacheStaleHits();

        float getCacheHitRatio();

        int getCacheMisses();

        float getCacheMissRatio();

        void flush();

        void flush(String key);
    }

    static class CacheMBeanImpl implements CacheMBean
    {
        private Cache cache;

        CacheMBeanImpl(Cache cache)
        {
            this.cache = cache;
        }

        @Override
        public int getSize()
        {
            return cache.getSize();
        }

        @Override
        public int getCacheHits()
        {
            return cache.getCacheHits();
        }

        @Override
        public int getCacheStaleHits()
        {
            return cache.getCacheStaleHits();
        }

        @Override
        public float getCacheHitRatio()
        {
            return cache.getCacheHitRatio();
        }

        @Override
        public int getCacheMisses()
        {
            return cache.getCacheMisses();
        }

        @Override
        public float getCacheMissRatio()
        {
            return cache.getCacheMissRatio();
        }

        @Override
        public void flush()
        {
            cache.flush();
        }

        @Override
        public void flush(String key)
        {
            cache.flush(key);
        }
    }
}
