package se.kuseman.payloadbuilder.core.cache;

import static org.apache.commons.lang3.StringUtils.isAllBlank;

import java.lang.management.ManagementFactory;
import java.time.ZonedDateTime;

import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.ObjectName;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Utils for working with cache and MBeans */
public class MBeanUtils
{
    /** Register cache to mbean server */
    public static void registerCacheMBean(CacheType type, String providerName, QualifiedName name, Cache cache)
    {
        try
        {
            ObjectName objectName = ObjectName.getInstance(String.format("se.kuseman.payloadbuilder:type=Cache,CacheType=%s%s,Name=%s", StringUtils.capitalize(type.name()
                    .toLowerCase()), !isAllBlank(providerName) ? ",Provider=%s".formatted(providerName)
                            : "",
                    name));
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

        String getLastAccessTime();

        String getLastReloadTime();

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
        public String getLastAccessTime()
        {
            ZonedDateTime lastAccessTime = cache.getLastAccessTime();
            return lastAccessTime != null ? lastAccessTime.toString()
                    : null;
        }

        @Override
        public String getLastReloadTime()
        {
            ZonedDateTime lastReloadTime = cache.getLastReloadTime();
            return lastReloadTime != null ? lastReloadTime.toString()
                    : null;
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
