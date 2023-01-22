package se.kuseman.payloadbuilder.core.cache;

import static java.util.Collections.emptyList;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.cache.AInMemoryCache.CacheImpl;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Test of {@link InMemoryTempTableCache} */
public class InMemoryTempTableCacheTest extends Assert
{
    @Test
    public void test_reload() throws InterruptedException
    {
        InMemoryTempTableCache cache = new InMemoryTempTableCache();

        CacheImpl<TemporaryTable> cacheImpl = cache.getCacheOrCreate(QualifiedName.of("table"));

        TupleVector first = TupleVector.EMPTY;
        TupleVector second = TupleVector.EMPTY;
        TupleVector third = TupleVector.EMPTY;

        AtomicInteger i = new AtomicInteger();
        Supplier<TemporaryTable> supplier = () ->
        {
            int call = i.getAndIncrement();

            if (call == 0)
            {
                return new TemporaryTable(first, emptyList());
            }
            else if (call == 1)
            {
                return new TemporaryTable(second, emptyList());
            }
            else if (call == 2)
            {
                throw new RuntimeException("fail");
            }

            return new TemporaryTable(third, emptyList());
        };

        assertEquals(0, cacheImpl.getCacheHits());
        TemporaryTable table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(first, table.getTupleVector());
        assertEquals(0, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(0, cacheImpl.getCacheStaleHits());

        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(first, table.getTupleVector());
        assertEquals(1, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(0, cacheImpl.getCacheStaleHits());

        Thread.sleep(150);

        // Key expired and reload is performed and we get the stale "old" data back
        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(first, table.getTupleVector());
        assertEquals(1, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(1, cacheImpl.getCacheStaleHits());

        // Let executor get value
        Thread.sleep(10);

        // A second call will yield the new result
        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(second, table.getTupleVector());
        assertEquals(2, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(1, cacheImpl.getCacheStaleHits());

        Thread.sleep(150);

        // Key expired and reload will fail and we should get second result back
        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(second, table.getTupleVector());
        assertEquals(2, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(2, cacheImpl.getCacheStaleHits());

        // Let executor get value
        Thread.sleep(10);

        // A second call will still yield second result
        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(second, table.getTupleVector());
        assertEquals(2, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(3, cacheImpl.getCacheStaleHits());

        Thread.sleep(150);

        // Key expired and reload will kick in and we should get second result back
        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(second, table.getTupleVector());
        assertEquals(2, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(4, cacheImpl.getCacheStaleHits());

        // Let executor get value
        Thread.sleep(10);

        // A second call will yield the new result
        table = cacheImpl.computeIfAbsent(QualifiedName.of("table"), Duration.ofMillis(100), supplier);
        assertSame(third, table.getTupleVector());
        assertEquals(3, cacheImpl.getCacheHits());
        assertEquals(1, cacheImpl.getCacheMisses());
        assertEquals(4, cacheImpl.getCacheStaleHits());
    }
}
