package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.CacheProvider;
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

    @Test(expected = OperatorException.class)
    public void test_bad_session()
    {
        new OuterValuesCacheOperator(
                1,
                op(ctx -> RowIterator.EMPTY),
                e("'PT10m'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')")).open(new ExecutionContext(session));
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
                e("'PT10m'"),
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        session.setCacheProvider(cacheProvider);

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
        assertEquals(4, cacheProvider.cache.size());
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
                LiteralNullExpression.NULL_LITERAL,
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        cacheProvider.cache.put(asList(2, "some-value", "const"), asList(Row.of(inner, 20, new Object[] {2})));

        session.setCacheProvider(cacheProvider);

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
        assertEquals(4, cacheProvider.cache.size());

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
                LiteralNullExpression.NULL_LITERAL,
                e("listOf(a.col, @param, 'const')"),
                e("listOf(b.col, @param, 'const')"));

        TestCacheProvider cacheProvider = new TestCacheProvider();
        cacheProvider.cache.put(asList(1, "some-value", "const"), asList(Row.of(inner, 0, new Object[] {1})));
        cacheProvider.cache.put(asList(2, "some-value", "const"), asList(Row.of(inner, 10, new Object[] {2})));
        cacheProvider.cache.put(asList(3, "some-value", "const"), asList(Row.of(inner, 20, new Object[] {3})));
        cacheProvider.cache.put(asList(4, "some-value", "const"), asList(Row.of(inner, 30, new Object[] {4})));

        session.setCacheProvider(cacheProvider);

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
        assertEquals(4, cacheProvider.cache.size());
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
    private class TestCacheProvider implements CacheProvider
    {
        Map<Object, List<Tuple>> cache = new HashMap<>();
        Duration ttl;

        @Override
        public <TKey> Map<TKey, List<Tuple>> getAll(Iterable<TKey> keys)
        {
            Map<TKey, List<Tuple>> result = new HashMap<>();
            for (TKey key  : keys)
            {
                Object cacheKey = key;
                if (cacheKey instanceof CacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }

                result.put(key, cache.get(cacheKey));
            }
            return result;
        }

        @Override
        public <TKey> void putAll(Map<TKey, List<Tuple>> values, Duration ttl)
        {
            this.ttl = ttl;
            for (Entry<TKey, List<Tuple>> entry : values.entrySet())
            {
                Object cacheKey = entry.getKey();
                if (cacheKey instanceof CacheProvider.CacheKey)
                {
                    cacheKey = ((CacheKey) cacheKey).getKey();
                }
                cache.put(cacheKey, entry.getValue());
            }
        }
    }
}
