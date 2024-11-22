package se.kuseman.payloadbuilder.core.cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
    public void test_perf() throws InterruptedException
    {
        InMemoryGenericCache cache = new InMemoryGenericCache("cache", false, false);
        CacheImpl<Object> cacheImpl = cache.getCacheOrCreate(QualifiedName.of("cache"));

        Random r = new Random(System.nanoTime());
        int iterations = 1000;
        int threadCount = 1000;
        List<Thread> threads = new ArrayList<>(threadCount);

        AtomicInteger count = new AtomicInteger();
        // long time = System.nanoTime();
        for (int i = 0; i < threadCount; i++)
        {
            Thread t = new Thread(() ->
            {
                for (int j = 0; j < iterations; j++)
                {
                    count.incrementAndGet();
                    assertEquals("value", cacheImpl.computeIfAbsent("key", Duration.ofMillis(100), () ->
                    {
                        try
                        {
                            Thread.sleep(r.nextInt(100));
                        }
                        catch (InterruptedException e)
                        {
                            e.printStackTrace();
                        }
                        return "value";
                    }));
                }
            });
            threads.add(t);
            t.start();
        }

        for (int i = 0; i < threadCount; i++)
        {
            threads.get(i)
                    .join();
        }

        /*
         * Before
         * 
         * CacheHitRatio: 1.0 CacheHits: 67173285 CacheMissRatio: 1.48868695E-8 CacheMisses: 1 CacheStaleHits: 32826714 100000000: 00:00:45.126
         * 
         * After
         * 
         * CacheHitRatio: 0.99999994 CacheHits: 29995106 CacheMissRatio: 3.3338768E-8 CacheMisses: 1 CacheStaleHits: 24889156 100000000: 00:00:06.080
         */

        long total = cacheImpl.getCacheHits() + cacheImpl.getCacheMisses() + cacheImpl.getCacheStaleHits();
        assertEquals(iterations * threadCount, total);

        // System.out.println("CacheHitRatio: " + cacheImpl.getCacheHitRatio());
        // System.out.println("CacheHits: " + cacheImpl.getCacheHits());
        // System.out.println("CacheMissRatio: " + cacheImpl.getCacheMissRatio());
        // System.out.println("CacheMisses: " + cacheImpl.getCacheMisses());
        // System.out.println("CacheStaleHits: " + cacheImpl.getCacheStaleHits());
        //
        // System.out.println(count + ": " + DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS)));
    }

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
