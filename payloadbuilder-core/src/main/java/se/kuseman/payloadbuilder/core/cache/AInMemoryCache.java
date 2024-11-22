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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kuseman.payloadbuilder.api.QualifiedName;

/**
 * Base class for simple in memory based caches. Implemented using a simple ConcurrentHashMap
 *
 * @param <TValue> Type of value in the cache
 */
abstract class AInMemoryCache<TValue> implements CacheProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AInMemoryCache.class);

    private final CacheType cacheType;
    /** Name of this cache instance. */
    private final String providerName;
    private final boolean enableJmx;
    /**
     * Sets if all entries should be loaded async even when the cache entry is not found in cache. This is useful if the user of the cache never want's any load time on the caller thread.
     */
    private final boolean alwaysLoadAsync;
    protected final Map<QualifiedName, CacheImpl<TValue>> caches = new ConcurrentHashMap<>();

    AInMemoryCache(CacheType cacheType, String providerName, boolean enableJmx, boolean alwaysLoadAsync)
    {
        this.cacheType = cacheType;
        this.providerName = providerName;
        this.enableJmx = enableJmx;
        this.alwaysLoadAsync = alwaysLoadAsync;
    }

    @Override
    public String getName()
    {
        return providerName;
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
            CacheImpl<TValue> result = new CacheImpl<>(name, alwaysLoadAsync);

            if (enableJmx)
            {
                MBeanUtils.registerCacheMBean(cacheType, AInMemoryCache.this.providerName, name, result);
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
        AtomicInteger cacheHits = new AtomicInteger();
        AtomicInteger cacheStaleHits = new AtomicInteger();
        AtomicInteger cacheMisses = new AtomicInteger();
        volatile ZonedDateTime lastAccessTime;
        volatile ZonedDateTime lastReloadTime;
        private boolean alwaysLoadAsync;

        CacheImpl(QualifiedName name, boolean alwaysLoadAsync)
        {
            this.name = name;
            this.elements = new ConcurrentHashMap<>();
            this.alwaysLoadAsync = alwaysLoadAsync;
        }

        /** Compute or get value for key */
        T computeIfAbsent(Object key, final Duration ttl, final Supplier<T> supplier)
        {
            // To save some locking/contention we do a
            // simple get to see if the value is returnable without
            // going into the compute section
            CacheEntryImpl<T> ce = elements.get(key);
            if (ce != null)
            {
                T value = ce.value;
                boolean expired = ce.isExpired();
                boolean reloading = ce.reloading;

                if (value != null
                        && (!expired
                                || reloading))
                {
                    lastAccessTime = ZonedDateTime.now();
                    if (reloading)
                    {
                        cacheStaleHits.incrementAndGet();
                    }
                    else
                    {
                        cacheHits.incrementAndGet();
                    }
                    return value;
                }
            }

            return elements.compute(key, (k, v) ->
            {
                lastAccessTime = ZonedDateTime.now();
                // Call supplier sync
                if (v == null
                        && !alwaysLoadAsync)
                {
                    cacheMisses.incrementAndGet();
                    return new CacheEntryImpl<>(k, supplier.get(), ttl);
                }

                boolean forceReload = v == null;
                CacheEntryImpl<T> entry = forceReload ? new CacheEntryImpl<>(key, null, ttl)
                        : v;

                // Return old value but reload in background
                if (entry.isExpired()
                        || forceReload)
                {
                    // Fetch new value in background and return old data
                    if (!entry.reloading)
                    {
                        lastReloadTime = ZonedDateTime.now();
                        entry.reloading = true;
                        EXECUTOR.submit(() ->
                        {
                            try
                            {
                                entry.value = supplier.get();
                                entry.setExpire(ttl);
                            }
                            catch (Throwable e)
                            {
                                // Swallow. We keep the old data in cache and return it.
                                // Reload will be retried next time the cache entry is requested
                                LOGGER.error("Error reloading cache: {}, key: {}", name, key, e);
                            }
                            finally
                            {
                                entry.reloading = false;
                            }
                        });
                    }

                    cacheStaleHits.incrementAndGet();
                }
                else
                {
                    cacheHits.incrementAndGet();
                }

                return entry;
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
            return cacheHits.get();
        }

        @Override
        public int getCacheStaleHits()
        {
            return cacheStaleHits.get();
        }

        @Override
        public int getCacheMisses()
        {
            return cacheMisses.get();
        }

        @Override
        public ZonedDateTime getLastAccessTime()
        {
            return lastAccessTime;
        }

        @Override
        public ZonedDateTime getLastReloadTime()
        {
            return lastReloadTime;
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
                    && System.nanoTime() >= expireTime;
        }

        void setExpire(Duration ttl)
        {
            this.expireTime = ttl != null ? (System.nanoTime() + ttl.toNanos())
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
