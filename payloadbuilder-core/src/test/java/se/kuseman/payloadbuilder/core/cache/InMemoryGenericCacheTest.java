package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.commons.lang3.ThreadUtils;
import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.core.cache.AInMemoryCache.CacheImpl;

/** Test of {@link InMemoryGenericCache} */
public class InMemoryGenericCacheTest extends Assert
{
    @Test
    public void test_always_load_async() throws InterruptedException
    {
        InMemoryGenericCache cache = new InMemoryGenericCache("cache", false, true);
        CacheImpl<Object> cacheImpl = cache.getCacheOrCreate(QualifiedName.of("cache"));

        AtomicInteger i = new AtomicInteger();
        Supplier<Object> supplier = () ->
        {
            ThreadUtils.sleepQuietly(Duration.ofMillis(1));

            int call = i.getAndIncrement();
            if (call == 0)
            {
                return "value";
            }
            else if (call == 1)
            {
                return "value1";
            }
            return "value2";
        };

        assertEquals(0, cacheImpl.getCacheHits());
        // Null on return since we always load async
        Object val = cacheImpl.computeIfAbsent("key", Duration.ofMillis(200), supplier);
        assertNull(val);
        assertEquals(0, cacheImpl.getCacheHits());
        assertEquals(0, cacheImpl.getCacheMisses());
        assertEquals(1, cacheImpl.getCacheStaleHits());

        // Let executor get value
        Thread.sleep(50);

        // First value back on next call when we loaded async
        val = cacheImpl.computeIfAbsent("key", Duration.ofMillis(200), supplier);
        assertEquals("value", val);
        assertEquals(1, cacheImpl.getCacheHits());
        assertEquals(0, cacheImpl.getCacheMisses());
        assertEquals(1, cacheImpl.getCacheStaleHits());

        // Expire key => first value back
        Thread.sleep(250);
        val = cacheImpl.computeIfAbsent("key", Duration.ofMillis(200), supplier);
        assertEquals("value", val);
        assertEquals(1, cacheImpl.getCacheHits());
        assertEquals(0, cacheImpl.getCacheMisses());
        assertEquals(2, cacheImpl.getCacheStaleHits());

        // Let executor get new value
        Thread.sleep(10);

        // New value back on next call
        val = cacheImpl.computeIfAbsent("key", Duration.ofMillis(200), supplier);
        assertEquals("value1", val);
        assertEquals(2, cacheImpl.getCacheHits());
        assertEquals(0, cacheImpl.getCacheMisses());
        assertEquals(2, cacheImpl.getCacheStaleHits());
    }
}
