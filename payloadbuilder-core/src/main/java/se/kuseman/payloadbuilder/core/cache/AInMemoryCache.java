package se.kuseman.payloadbuilder.core.cache;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;

/**
 * Base class for simple in memory based caches. Implemented using a simple ConcurrentHashMap
 *
 * @param <TValue> Type of value in the cache
 */
abstract class AInMemoryCache<TValue> implements CacheProvider
{
    private final CacheType cacheType;
    private final boolean enableJmx;
    protected final Map<QualifiedName, CacheImpl<TValue>> caches = new ConcurrentHashMap<>();

    AInMemoryCache(CacheType cacheType, boolean enableJmx)
    {
        this.cacheType = cacheType;
        this.enableJmx = enableJmx;
    }

    @Override
    public String getName()
    {
        return "InMemory";
    }

    @Override
    public List<Cache> getCaches()
    {
        return new ArrayList<>(caches.values());
    }

    @Override
    public void flushAll()
    {
        for (CacheImpl<TValue> e : caches.values())
        {
            e.elements.clear();
        }
    }

    @Override
    public void remove(QualifiedName name)
    {
        caches.remove(name);
    }

    @Override
    public void removeAll()
    {
        caches.clear();
    }

    @Override
    public Cache getCache(QualifiedName name)
    {
        return caches.get(name);
    }

    protected CacheImpl<TValue> getCacheOrCreate(QualifiedName name)
    {
        requireNonNull(name, "name of cache is mandatory");
        return caches.computeIfAbsent(name, k ->
        {
            CacheImpl<TValue> result = new CacheImpl<>(name);

            if (enableJmx)
            {
                MBeanUtils.registerCacheMBean(cacheType, name, result);
            }

            return result;
        });
    }

    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(new ThreadFactory()
    {
        AtomicInteger threadCount = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r)
        {
            String name = AInMemoryCache.class.getSimpleName() + "#CacheReloader#" + threadCount.incrementAndGet();
            Thread t = new Thread(r, name);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    });

    /**
     * Cache
     *
     * @param <T> Type of object in cache
     */
    static class CacheImpl<T> implements Cache
    {
        final QualifiedName name;
        final Map<Object, CacheEntryImpl<T>> elements;
        int cacheHits;
        int cacheStaleHits;
        int cacheMisses;

        CacheImpl(QualifiedName name)
        {
            this.name = name;
            this.elements = new ConcurrentHashMap<>();
        }

        /** Compute or get value for key */
        T computeIfAbsent(Object key, final Duration ttl, final Supplier<T> supplier)
        {
            return elements.compute(key, (k, v) ->
            {
                // Call supplier sync
                if (v == null)
                {
                    cacheMisses++;
                    return new CacheEntryImpl<>(k, supplier.get(), ttl);
                }
                // Return old value but reload in background
                else if (v.isExpired())
                {
                    // Fetch new value in background and return old data
                    if (!v.reloading)
                    {
                        v.reloading = true;
                        EXECUTOR.submit(() ->
                        {
                            try
                            {
                                v.value = supplier.get();
                                v.setExpire(ttl);
                            }
                            catch (Exception e)
                            {
                                // Swallow. We keep the old data in cache and return in
                                // and retry the supplier next time the cache entry is requested
                            }
                            finally
                            {
                                v.reloading = false;
                            }
                        });
                    }

                    cacheStaleHits++;
                    return v;
                }

                cacheHits++;
                return v;
            }).value;
        }

        @Override
        public QualifiedName getName()
        {
            return name;
        }

        @Override
        public int getSize()
        {
            return elements.size();
        }

        @Override
        public int getCacheHits()
        {
            return cacheHits;
        }

        @Override
        public int getCacheStaleHits()
        {
            return cacheStaleHits;
        }

        @Override
        public int getCacheMisses()
        {
            return cacheMisses;
        }

        @Override
        public void flush()
        {
            elements.clear();
        }

        @Override
        public void flush(Object key)
        {
            elements.remove(key);
        }

        @Override
        public List<CacheEntry> getCacheEntries()
        {
            return new ArrayList<>(elements.values());
        }
    }

    /**
     * Cache entry
     *
     * @param <T> Type of object in cache
     */
    static class CacheEntryImpl<T> implements Cache.CacheEntry
    {
        Object key;
        volatile long expireTime;
        volatile T value;
        volatile boolean reloading = false;

        CacheEntryImpl(Object key, T value, Duration ttl)
        {
            this.key = key;
            this.value = value;
            setExpire(ttl);
        }

        boolean isExpired()
        {
            return expireTime >= 0
                    && System.currentTimeMillis() >= expireTime;
        }

        void setExpire(Duration ttl)
        {
            this.expireTime = ttl != null ? (System.currentTimeMillis() + ttl.toMillis())
                    : -1;
        }

        @Override
        public Object getKey()
        {
            return key;
        }

        @Override
        public ZonedDateTime getExpireTime()
        {
            return expireTime >= 0 ? ZonedDateTime.ofInstant(Instant.ofEpochMilli(expireTime), ZoneId.systemDefault())
                    : null;
        }
    }
}
