package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.cache.BatchCacheProvider;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.BatchCacheOperator.CacheSettings;
import org.kuse.payloadbuilder.core.operator.IIndexValuesFactory.IIndexValues;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link BatchCacheOperator} */
public class BatchCacheOperatorTest extends AOperatorTest
{
    private final TableAlias inner = TableAlias.TableAliasBuilder.of(1, TableAlias.Type.TABLE, QualifiedName.of("tableB"), "b")
            .tableMeta(new TableMeta(asList(new TableMeta.Column("col", DataType.ANY))))
            .build();

    @Test(expected = OperatorException.class)
    public void test_bad_cache_name_expression()
    {
        context.getSession().setBatchCacheProvider(new TestCacheProvider());
        new BatchCacheOperator(
                1,
                op1(ctx -> TupleIterator.EMPTY),
                new ExpressionIndexValuesFactory(asList(e("a.col"))),
                new CacheSettings(
                        ctx -> null,
                        ctx -> asList(1, "const"),
                        ctx -> "PT10m"))
                                .open(new ExecutionContext(session));
    }

    @Test
    public void test()
    {
        MutableBoolean closeCalled = new MutableBoolean(false);
        Operator downstream = new Operator()
        {
            @Override
            public TupleIterator open(ExecutionContext context)
            {
                return new TupleIterator()
                {
                    private int pos = -1;

                    @Override
                    public Tuple next()
                    {
                        IIndexValues next = context.getStatementContext().getOuterIndexValues().next();
                        pos++;
                        return Row.of(inner, 10 * pos, new Object[] {next.getValue(0)});
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return context.getStatementContext().getOuterIndexValues().hasNext();
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

        BatchCacheOperator op = new BatchCacheOperator(
                1,
                downstream,
                new ExpressionIndexValuesFactory(asList(e("b.col", inner))),
                new CacheSettings(
                        ctx -> "article",
                        ctx -> "const",
                        ctx -> "PT10m"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        session.setBatchCacheProvider(cacheProvider);

        // Prepare outer values for cache/downstream
        context.getStatementContext()
                .setOuterIndexValues(asList(
                        (IIndexValues) new ExpressionIndexValuesFactory.IndexValues(new Object[] {1}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {2}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {3}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {4})).iterator());

        TupleIterator it = op.open(context);
        assertEquals(4, cacheProvider.cache.get("test_article").size());
        assertEquals(Duration.of(10, ChronoUnit.MINUTES), cacheProvider.ttl);
        assertTrue(closeCalled.booleanValue());

        int[] expectedInnerPositions = new int[] {0, 10, 20, 30};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerPositions[count], (int) tuple.getValue(Row.POS_ORDINAL), 0);
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
            public TupleIterator open(ExecutionContext context)
            {
                operatorCalls.increment();
                // Skip one row

                MutableInt pos = new MutableInt(0);
                pos.incrementAndGet();
                context.getStatementContext().getOuterIndexValues().next();

                return new TupleIterator()
                {
                    @Override
                    public Tuple next()
                    {
                        context.getStatementContext().getOuterIndexValues().next();
                        pos.incrementAndGet();
                        return Row.of(inner, 10 * pos.intValue(), new Object[] {pos.getValue()});
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return context.getStatementContext().getOuterIndexValues().hasNext();
                    }
                };
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        BatchCacheOperator op = new BatchCacheOperator(
                1,
                downstream,
                new ExpressionIndexValuesFactory(asList(e("b.col", inner))),
                new CacheSettings(
                        ctx -> "article",
                        null,
                        ctx -> "PT10m"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        session.setBatchCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        // Prepare outer values for cache/downstream
        context.getStatementContext()
                .setOuterIndexValues(asList(
                        (IIndexValues) new ExpressionIndexValuesFactory.IndexValues(new Object[] {1}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {2}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {3}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {4})).iterator());

        TupleIterator it = op.open(context);
        assertEquals(4, cacheProvider.cache.get("test_article").size());
        assertEquals(Duration.of(10, ChronoUnit.MINUTES), cacheProvider.ttl);
        assertEquals(1, operatorCalls.intValue());

        int[] expectedInnerPositions = new int[] {20, 30, 40};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals("Count: " + count, expectedInnerPositions[count], tuple.getValue(Row.POS_ORDINAL));
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
            public TupleIterator open(ExecutionContext context)
            {
                return new TupleIterator()
                {
                    private int pos = -1;

                    @Override
                    public Tuple next()
                    {
                        IIndexValues next = context.getStatementContext().getOuterIndexValues().next();
                        pos++;
                        return Row.of(inner, 10 * pos, new Object[] {next.getValue(0)});
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return context.getStatementContext().getOuterIndexValues().hasNext();
                    }
                };
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }
        };

        BatchCacheOperator op = new BatchCacheOperator(
                1,
                downstream,
                new ExpressionIndexValuesFactory(asList(e("b.col", inner))),
                new CacheSettings(
                        ctx -> "inner",
                        ctx -> "value",
                        ctx -> null));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        Map<Object, List<Tuple>> cache = new HashMap<>();
        cacheProvider.cache.put("test_inner", cache);
        cache.put(new BatchCacheOperator.CacheKey("value", new ExpressionIndexValuesFactory.IndexValues(new Object[] {2})), asList(Row.of(inner, 20, new Object[] {2})));

        session.setBatchCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);

        // Prepare outer values for cache/downstream
        context.getStatementContext()
                .setOuterIndexValues(asList(
                        (IIndexValues) new ExpressionIndexValuesFactory.IndexValues(new Object[] {1}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {2}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {3}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {4})).iterator());

        TupleIterator it = op.open(context);
        assertEquals(4, cache.size());

        // First the cache value, then the 3 down stream tuples
        int[] expectedInnerValues = new int[] {2, 1, 3, 4};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals("Count: " + count, expectedInnerValues[count], getValue(tuple, -1, "col"));
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
            public TupleIterator open(ExecutionContext context)
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

        BatchCacheOperator op = new BatchCacheOperator(
                1,
                downstream,
                new ExpressionIndexValuesFactory(asList(e("b.col", inner))),
                new CacheSettings(
                        ctx -> "inner",
                        ctx -> asList(1, "const"),
                        ctx -> null));

        TestCacheProvider cacheProvider = new TestCacheProvider();

        Map<Object, List<Tuple>> cache = new HashMap<>();
        cacheProvider.cache.put("test_inner", cache);
        cache.put(new BatchCacheOperator.CacheKey(asList(1, "const"), new ExpressionIndexValuesFactory.IndexValues(new Object[] {1})), asList(Row.of(inner, 0, new Object[] {1})));
        cache.put(new BatchCacheOperator.CacheKey(asList(1, "const"), new ExpressionIndexValuesFactory.IndexValues(new Object[] {2})), asList(Row.of(inner, 10, new Object[] {2})));
        cache.put(new BatchCacheOperator.CacheKey(asList(1, "const"), new ExpressionIndexValuesFactory.IndexValues(new Object[] {3})), asList(Row.of(inner, 20, new Object[] {3})));
        cache.put(new BatchCacheOperator.CacheKey(asList(1, "const"), new ExpressionIndexValuesFactory.IndexValues(new Object[] {4})), asList(Row.of(inner, 30, new Object[] {4})));

        session.setBatchCacheProvider(cacheProvider);

        ExecutionContext context = new ExecutionContext(session);
        context.setVariable("param", "some-value");

        // Prepare outer values for cache/downstream
        context.getStatementContext()
                .setOuterIndexValues(asList(
                        (IIndexValues) new ExpressionIndexValuesFactory.IndexValues(new Object[] {1}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {2}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {3}),
                        new ExpressionIndexValuesFactory.IndexValues(new Object[] {4})).iterator());

        TupleIterator it = op.open(context);
        assertEquals(4, cache.size());
        int[] expectedInnerValues = new int[] {1, 2, 3, 4};
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedInnerValues[count], getValue(tuple, -1, "col"));
            count++;
        }

        assertEquals(4, count);
    }

    /** Test provider */
    static class TestCacheProvider implements BatchCacheProvider
    {
        Map<String, Map<Object, List<Tuple>>> cache = new HashMap<>();
        Duration ttl;

        @Override
        public String getName()
        {
            return "Test provider";
        }

        @Override
        public <TKey> Map<TKey, List<Tuple>> getAll(QualifiedName cacheName, Iterable<TKey> keys)
        {
            Map<Object, List<Tuple>> currentCache = cache.computeIfAbsent("test_" + cacheName, k -> new HashMap<>());
            Map<TKey, List<Tuple>> result = new HashMap<>();
            for (TKey key : keys)
            {
                Object cacheKey = key;
                result.put(key, currentCache.get(cacheKey));
            }
            return result;
        }

        @Override
        public <TKey> void putAll(QualifiedName cacheName, Map<TKey, List<Tuple>> values, Duration ttl)
        {
            this.ttl = ttl;
            Map<Object, List<Tuple>> currentCache = cache.computeIfAbsent("test_" + cacheName, k -> new HashMap<>());
            for (Entry<TKey, List<Tuple>> entry : values.entrySet())
            {
                TKey cacheKey = entry.getKey();
                currentCache.put(cacheKey, entry.getValue());
            }
        }

        @Override
        public void flushAll()
        {
        }

        @Override
        public void flush(QualifiedName name)
        {
        }

        @Override
        public void flush(QualifiedName name, Object key)
        {
        }

        @Override
        public void removeAll()
        {
        }

        @Override
        public void remove(QualifiedName name)
        {
        }
    }
}
