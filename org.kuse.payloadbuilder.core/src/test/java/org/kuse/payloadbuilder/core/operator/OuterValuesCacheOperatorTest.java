package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.TupleCacheProvider;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.OperatorContext.OuterValues;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.LiteralNullExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link OuterValuesCacheOperator} */
public class OuterValuesCacheOperatorTest extends AOperatorTest
{
    private final TableAlias outer = TableAlias.TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA"), "a").columns(new String[] {"col"}).build();
    private final TableAlias inner = TableAlias.TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableB"), "b").columns(new String[] {"col"}).build();
    private final CacheCatalog catalog = new CacheCatalog();

    /** Test catalog with cache support */
    private class CacheCatalog extends Catalog
    {
        CacheCatalog()
        {
            super("CacheTest");
        }

        @Override
        public String prepareTupleCacheName(ExecutionContext context, String catalogAlias, String cacheName)
        {
            return "test_" + cacheName;
        }
    }

    @Test(expected = OperatorException.class)
    public void test_bad_session()
    {
        new OuterValuesCacheOperator(
                1,
                op(ctx -> RowIterator.EMPTY),
                catalog,
                "alias",
                e("'inner'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"),
                e("'PT10m'")).open(new ExecutionContext(session));
    }

    @Test
    public void test_no_outer_values()
    {
        MutableBoolean closeCalled = new MutableBoolean(false);
        Operator downstream = op(ctx -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(inner, i, new Object[] {i})).iterator(), () -> closeCalled.setTrue());

        OuterValuesCacheOperator op = new OuterValuesCacheOperator(
                1,
                downstream,
                catalog,
                "alias",
                e("'article'"),
                e("5"),
                e("listOf(b.col, @param, 'const')"),
                e("'PT10m'"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        session.setTupleCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        RowIterator it = op.open(context);
        // All 9 inner values should be cached
        assertEquals(10, cacheProvider.cache.get("test_article").size());
        assertEquals(Duration.of(10, ChronoUnit.MINUTES), cacheProvider.ttl);
        assertTrue(closeCalled.booleanValue());

        int[] expectedInnerValues = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerValues[count], tuple.getValue(QualifiedName.of("b", "col"), 0));
            count++;
        }

        assertEquals(9, count);
    }

    @Test
    public void test()
    {
        MutableBoolean closeCalled = new MutableBoolean(false);
        Operator downstream = new Operator()
        {
            @Override
            public RowIterator open(ExecutionContext context)
            {
                return new RowIterator()
                {
                    private int pos = -1;

                    @Override
                    public Tuple next()
                    {
                        OuterValues next = context.getOperatorContext().getOuterIndexValues().next();
                        pos++;
                        return Row.of(inner, 10 * pos, next.getValues());
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return context.getOperatorContext().getOuterIndexValues().hasNext();
                    }

                    @Override
                    public void close()
                    {
                        closeCalled.setTrue();
                    }
                };
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        OuterValuesCacheOperator op = new OuterValuesCacheOperator(
                1,
                downstream,
                catalog,
                "alias",
                e("'article'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"),
                e("'PT10m'"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        session.setTupleCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        // Prepare outer values for cache/downstream
        context.getOperatorContext()
                .setOuterIndexValues(asList(
                        ov(Row.of(outer, 0, new Object[] {1})),
                        ov(Row.of(outer, 1, new Object[] {2})),
                        ov(Row.of(outer, 2, new Object[] {3})),
                        ov(Row.of(outer, 3, new Object[] {4}))).iterator());

        RowIterator it = op.open(context);
        assertEquals(4, cacheProvider.cache.get("test_article").size());
        assertEquals(Duration.of(10, ChronoUnit.MINUTES), cacheProvider.ttl);
        assertTrue(closeCalled.booleanValue());

        int[] expectedInnerValues = new int[] {2, 3, 4, 1};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerValues[count], tuple.getValue(QualifiedName.of("b", "col"), 0));
            count++;
        }

        assertEquals(4, count);
    }

    @Test
    public void test_missing_tuples_are_cached()
    {
        MutableInt operatorCalls = new MutableInt(0);
        Operator downstream = new Operator()
        {
            @Override
            public RowIterator open(ExecutionContext context)
            {
                operatorCalls.increment();
                // Skip one row

                AtomicInteger pos = new AtomicInteger(0);
                pos.incrementAndGet();
                context.getOperatorContext().getOuterIndexValues().next();

                return new RowIterator()
                {
                    @Override
                    public Tuple next()
                    {
                        context.getOperatorContext().getOuterIndexValues().next();
                        pos.incrementAndGet();
                        return Row.of(inner, 10 * pos.get(), new Object[] {pos.get()});
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return context.getOperatorContext().getOuterIndexValues().hasNext();
                    }
                };
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        OuterValuesCacheOperator op = new OuterValuesCacheOperator(
                1,
                downstream,
                catalog,
                "alias",
                e("'article'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"),
                e("'PT10m'"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        session.setTupleCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        // Prepare outer values for cache/downstream
        context.getOperatorContext()
                .setOuterIndexValues(asList(
                        ov(Row.of(outer, 0, new Object[] {1})),
                        ov(Row.of(outer, 1, new Object[] {2})),
                        ov(Row.of(outer, 2, new Object[] {3})),
                        ov(Row.of(outer, 3, new Object[] {4}))).iterator());

        RowIterator it = op.open(context);
        assertEquals(4, cacheProvider.cache.get("test_article").size());
        assertEquals(Duration.of(10, ChronoUnit.MINUTES), cacheProvider.ttl);
        assertEquals(1, operatorCalls.intValue());

        int[] expectedInnerValues = new int[] {2, 3, 4};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerValues[count], tuple.getValue(QualifiedName.of("b", "col"), 0));
            count++;
        }

        assertEquals(3, count);

        // Open once again, and down stream op should not be called again
        it = op.open(context);
        assertEquals(4, cacheProvider.cache.get("test_article").size());
        assertEquals(1, operatorCalls.intValue());
    }

    @Test
    public void test_with_partial_cached_data()
    {
        Operator downstream = new Operator()
        {
            @Override
            public RowIterator open(ExecutionContext context)
            {
                return new RowIterator()
                {
                    private int pos = -1;

                    @Override
                    public Tuple next()
                    {
                        OuterValues next = context.getOperatorContext().getOuterIndexValues().next();
                        pos++;
                        return Row.of(inner, 10 * pos, next.getValues());
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return context.getOperatorContext().getOuterIndexValues().hasNext();
                    }
                };
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        OuterValuesCacheOperator op = new OuterValuesCacheOperator(
                1,
                downstream,
                catalog,
                "alias",
                e("'inner'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"),
                LiteralNullExpression.NULL_LITERAL);

        TestCacheProvider cacheProvider = new TestCacheProvider();
        Map<Object, List<Tuple>> cache = new HashMap<>();
        cacheProvider.cache.put("test_inner", cache);
        cache.put(asList(2, "some-value", "const"), asList(Row.of(inner, 20, new Object[] {2})));

        session.setTupleCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        // Prepare outer values for cache/downstream
        context.getOperatorContext()
                .setOuterIndexValues(asList(
                        ov(Row.of(outer, 0, new Object[] {1})),
                        ov(Row.of(outer, 1, new Object[] {2})),
                        ov(Row.of(outer, 2, new Object[] {3})),
                        ov(Row.of(outer, 3, new Object[] {4}))).iterator());

        RowIterator it = op.open(context);
        assertEquals(4, cache.size());

        int[] expectedInnerValues = new int[] {2, 3, 4, 1};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerValues[count], tuple.getValue(QualifiedName.of("b", "col"), 0));
            count++;
        }

        assertEquals(4, count);
    }

    @Test
    public void test_with_full_cached_data()
    {
        Operator downstream = new Operator()
        {
            @Override
            public RowIterator open(ExecutionContext context)
            {
                // This should not be called
                throw new RuntimeException("NONO");
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        OuterValuesCacheOperator op = new OuterValuesCacheOperator(
                1,
                downstream,
                catalog,
                "alias",
                e("'inner'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"),
                LiteralNullExpression.NULL_LITERAL);

        TestCacheProvider cacheProvider = new TestCacheProvider();

        Map<Object, List<Tuple>> cache = new HashMap<>();
        cacheProvider.cache.put("test_inner", cache);
        cache.put(asList(1, "some-value", "const"), asList(Row.of(inner, 0, new Object[] {1})));
        cache.put(asList(2, "some-value", "const"), asList(Row.of(inner, 10, new Object[] {2})));
        cache.put(asList(3, "some-value", "const"), asList(Row.of(inner, 20, new Object[] {3})));
        cache.put(asList(4, "some-value", "const"), asList(Row.of(inner, 30, new Object[] {4})));

        session.setTupleCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        // Prepare outer values for cache/downstream
        context.getOperatorContext()
                .setOuterIndexValues(asList(
                        ov(Row.of(outer, 0, new Object[] {1})),
                        ov(Row.of(outer, 1, new Object[] {2})),
                        ov(Row.of(outer, 2, new Object[] {3})),
                        ov(Row.of(outer, 3, new Object[] {4}))).iterator());

        RowIterator it = op.open(context);
        assertEquals(4, cache.size());
        int[] expectedInnerValues = new int[] {2, 3, 4, 1};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerValues[count], tuple.getValue(QualifiedName.of("b", "col"), 0));
            count++;
        }

        assertEquals(4, count);
    }

    private OuterValues ov(Row tuple)
    {
        OuterValues ov = new OuterValues();
        ov.setOuterTuple(tuple);
        ov.setValues(tuple.getValues());
        return ov;
    }

    /** Test provider */
    private class TestCacheProvider implements TupleCacheProvider
    {
        Map<String, Map<Object, List<Tuple>>> cache = new HashMap<>();
        Duration ttl;

        @Override
        public <TKey> Map<TKey, List<Tuple>> getAll(String cacheName, Iterable<TKey> keys)
        {
            Map<TKey, List<Tuple>> result = new HashMap<>();
            for (TKey key : keys)
            {
                Object cacheKey = key;
                if (cacheKey instanceof TupleCacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }

                result.put(key, cache.computeIfAbsent(cacheName, k -> new HashMap<>()).get(cacheKey));
            }
            return result;
        }

        @Override
        public <TKey> void putAll(String cacheName, Map<TKey, List<Tuple>> values, Duration ttl)
        {
            this.ttl = ttl;
            for (Entry<TKey, List<Tuple>> entry : values.entrySet())
            {
                Object cacheKey = entry.getKey();
                if (cacheKey instanceof TupleCacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }
                cache.computeIfAbsent(cacheName, k -> new HashMap<>()).put(cacheKey, entry.getValue());
            }
        }
    }
}
