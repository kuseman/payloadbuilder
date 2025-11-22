package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.ArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.physicalplan.HashAggregate.GroupKey;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntHash;
import it.unimi.dsi.fastutil.ints.IntList;

/** Test of {@link HashAggregate} */
class HashAggregateTest extends APhysicalPlanTest
{
    @Test
    void test_invalid_input()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");
        IDatasource ds = schemaLessDS(() ->
        {
        });

        assertThrows(IllegalArgumentException.class, () -> new HashAggregate(0, scan(ds, table, schema), asList(ce("col1")), emptyList(), null));
    }

    @Test
    void test_invalid_input_2()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");
        IDatasource ds = schemaLessDS(() ->
        {
        });

        assertThrows(IllegalArgumentException.class, () -> new HashAggregate(0, scan(ds, table, schema), emptyList(), asList(new AggregateWrapperExpression(ce("col1"), false, false)), null));
    }

    @Test
    void test_distinct()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");

        IDatasource ds = schemaLessDS(() ->
        {
        }, TupleVector.of(schema, asList(vv(Type.Int, 1, null, 2, 3, 3, 3, 4, 4, 4, 4, null), vv(Type.Any, 1, 2, 2, 3, 3, 6, 4, 4, 9, 10, null))));

        //@formatter:off
        IPhysicalPlan plan =
                new Sort(2,
                        new HashAggregate(1, scan(ds, table, Schema.EMPTY), emptyList(), emptyList(), null),
                        asList(sortItem(ce("col1", 0), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        // Plan schema
        assertEquals(Schema.EMPTY, plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        // Runtime schema
        //@formatter:off
        assertEquals(Schema.of(
                col("col1", ResolvedType.of(Type.Int), table),
                col("col2", ResolvedType.of(Type.Any), table)), actual.getSchema());
        //@formatter:on

        //@formatter:off
        assertVectorsEquals(vv(Type.Int, null, null, 1, 2, 3, 3, 4, 4,  4), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, null, 2,    1, 2, 3, 6, 9, 10, 4), actual.getColumn(1));
        //@formatter:on
    }

    @Test
    void test_schema_less()
    {
        MutableBoolean closed = new MutableBoolean();

        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");

        IDatasource ds = schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Int, 1, 2, 2, 3, 3, 3, 4, 4, 4, 4), vv(Type.Any, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))));

        IExpression col1 = ce("col1", ResolvedType.of(Type.Int));
        IExpression col2 = ce("col2");

        //@formatter:off
        IPhysicalPlan plan =
                new Sort(2,
                        new HashAggregate(1, scan(ds, table, Schema.EMPTY), asList(col1),
                                asList(
                                        new FunctionCallExpression("", SystemCatalog.get().getScalarFunction("count"), null, asList(col1)),
                                        new AggregateWrapperExpression(col2, false, false),
                                        new FunctionCallExpression("", SystemCatalog.get().getScalarFunction("sum"), null, asList(col1)),
                                        new AggregateWrapperExpression(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, col1, col2), false, false)),
                                null),
                        asList(sortItem(ce("count", 0), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        // Plan schema
        //@formatter:off
        assertEquals(Schema.of(
                col(ResolvedType.of(Type.Int), "count(col1)"),
                col("col2", ResolvedType.array(ResolvedType.of(Type.Any))),
                col(ResolvedType.of(Type.Int), "sum(col1)"),
                col(ResolvedType.array(ResolvedType.of(Type.Int)), "col1 + col2")
                ),
                plan.getSchema());
        //@formatter:on

        TupleIterator it = plan.execute(context);
        TupleVector actual = it.next();
        assertFalse(it.hasNext());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }

        // Runtime schema (now we have data to return an INT that was object on plan level)
        //@formatter:off
        assertEquals(Schema.of(
                col(ResolvedType.of(Type.Int), "count(col1)"),
                col("col2", ResolvedType.array(ResolvedType.of(Type.Any))),
                col(ResolvedType.of(Type.Int), "sum(col1)"),
                col(ResolvedType.array(ResolvedType.of(Type.Int)), "col1 + col2")
                ),
                actual.getSchema());
        //@formatter:on
        assertEquals(4, actual.getRowCount());

        // Count
        assertVectorsEquals(vv(Type.Int, 1, 2, 3, 4), actual.getColumn(0));

        ValueVector vvCol1 = actual.getColumn(1);
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Any)), vvCol1.type());
        assertEquals(4, vvCol1.size());

        // Assert groups
        assertVectorsEquals(vv(Type.Any, 1), vvCol1.getArray(0));
        assertVectorsEquals(vv(Type.Any, 2, 3), vvCol1.getArray(1));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), vvCol1.getArray(2));
        assertVectorsEquals(vv(Type.Any, 7, 8, 9, 10), vvCol1.getArray(3));

        // Sum
        assertVectorsEquals(vv(Type.Int, 1, 4, 9, 16), actual.getColumn(2));
        assertTrue(closed.booleanValue());

        // Arithmetic
        ValueVector arith = actual.getColumn(3);
        assertEquals(ResolvedType.array(ResolvedType.of(Type.Int)), arith.type());
        assertEquals(4, arith.size());
        assertVectorsEquals(vv(Type.Int, 2), arith.getArray(0));
        assertVectorsEquals(vv(Type.Int, 4, 5), arith.getArray(1));
        assertVectorsEquals(vv(Type.Int, 7, 8, 9), arith.getArray(2));
        assertVectorsEquals(vv(Type.Int, 11, 12, 13, 14), arith.getArray(3));
    }

    /**
     * Measures the hash table implementation used in HashAggregate. JDK hashmap
     */
    @Disabled
    @Test
    void test_measure_hash_table_jdk() throws InterruptedException, ExecutionException
    {
        int rowCount = 1_000_000_000;

        final UTF8String[] strings = new UTF8String[413];
        for (int i = 0; i < strings.length; i++)
        {
            strings[i] = UTF8String.from(UUID.randomUUID()
                    .toString()
                    .getBytes());
        }

        ValueVector city = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public boolean hasNulls()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return rowCount;
            }

            @Override
            public UTF8String getString(int row)
            {
                int actualRow = row % strings.length;
                return strings[actualRow];
            }
        };

        int groupSize = 10_000;
        int groups = rowCount / groupSize;

        ValueVector[] rowValues = new ValueVector[] { city };
        Type[] types = new Type[] { Type.String };
        boolean[] hasNulls = new boolean[] { false };

        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime()
                .availableProcessors());

        for (int j = 0; j < 100; j++)
        {
            Map<GroupKey, IntList> table = new ConcurrentHashMap<>();
            List<Future<?>> futures = new ArrayList<>();

            long start = System.nanoTime();
            for (int k = 0; k < groups; k++)
            {
                final int startIndex = k * groupSize;
                final int stopIndex = startIndex + groupSize;
                // System.out.println("%d - %d".formatted(startIndex, stopIndex));
                futures.add(service.submit(() ->
                {
                    for (int i = startIndex; i < stopIndex; i++)
                    {
                        GroupKey key = new GroupKey(0, rowValues, types, hasNulls);
                        key.row = i;
                        IntList list = table.computeIfAbsent(key, kk -> new IntArrayList());
                        // IntList list =
                        //
                        // table.get(key);
                        // if (list == null)
                        // {
                        // list = new IntArrayList();
                        // GroupKey key1 = new GroupKey(0, rowValues, types, hasNulls);
                        // key1.row = i;
                        // table.put(key1, list);
                        // }
                        synchronized (list)
                        {
                            list.add(i);
                        }
                    }
                }));
            }

            for (Future<?> f : futures)
            {
                f.get();
            }

            System.out.println(table.size());
            System.out.println(table.values()
                    .stream()
                    .mapToInt(IntList::size)
                    .sum());
            System.out.println(DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }

    /**
     * Measures the hash table implementation used in HashAggregate.Int2ObjectOpenhashMap
     */
    @Disabled
    @Test
    void test_measure_hash_table_fastutil() throws InterruptedException, ExecutionException
    {
        int rowCount = 1_000_000_00;
        final UTF8String[] strings = new UTF8String[413];
        for (int i = 0; i < strings.length; i++)
        {
            strings[i] = UTF8String.from(UUID.randomUUID()
                    .toString()
                    .getBytes());
        }

        ValueVector city = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public boolean hasNulls()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return rowCount;
            }

            @Override
            public UTF8String getString(int row)
            {
                int actualRow = row % strings.length;
                return strings[actualRow];
            }
        };

        /* Table for all groups */
        // Object2ObjectMap<GroupKey, IntList> table = new Object2ObjectOpenCustomHashMap<>(new Hash.Strategy<GroupKey>()
        // {
        // @Override
        // public int hashCode(GroupKey o)
        // {
        // return o.hashCode();
        // }
        //
        // @Override
        // public boolean equals(GroupKey a, GroupKey b)
        // {
        // if (a == null
        // || b == null)
        // {
        // return a == b;
        // }
        //
        // return a.equals(b);
        // }
        // });

        int groupSize = 10_000;
        int groups = rowCount / groupSize;
        ExecutorService service = new ThreadPoolExecutor(Runtime.getRuntime()
                .availableProcessors() / 2,
                Runtime.getRuntime()
                        .availableProcessors() / 2,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(100), new ThreadPoolExecutor.CallerRunsPolicy());

        for (int j = 0; j < 100; j++)
        {

            List<Future<Int2ObjectMap<IntList>>> futures = new ArrayList<>();

            long start = System.nanoTime();
            for (int k = 0; k < groups; k++)
            {
                final int startIndex = k * groupSize;
                final int stopIndex = startIndex + groupSize;
                // System.out.println("%d - %d".formatted(startIndex, stopIndex));
                futures.add(service.submit(() ->
                {
                    Int2ObjectMap<IntList> table = new Int2ObjectOpenCustomHashMap<IntList>(new IntHash.Strategy()
                    {
                        @Override
                        public int hashCode(int e)
                        {
                            return city.getString(e)
                                    .hashCode();
                        }

                        @Override
                        public boolean equals(int a, int b)
                        {
                            return city.getString(a)
                                    .equals(city.getString(b));
                        }
                    });
                    for (int i = startIndex; i < stopIndex; i++)
                    {
                        // GroupKey key = new GroupKey(0, rowValues, types, hasNulls);
                        // key.row = i;
                        // IntList list = table.computeIfAbsent(i, kk -> new IntArrayList());
                        IntList list = table.get(i);
                        if (list == null)
                        {
                            list = new IntArrayList();
                            table.put(i, list);
                        }
                        // synchronized (list)
                        // {
                        list.add(i);
                        // }
                    }

                    return table;
                }));
            }

            int tableSize = 0;
            int totalRowCount = 0;
            for (Future<Int2ObjectMap<IntList>> f : futures)
            {
                Int2ObjectMap<IntList> map = f.get();
                tableSize += map.size();
                totalRowCount += map.values()
                        .stream()
                        .mapToInt(IntList::size)
                        .sum();
            }
            System.out.println(tableSize);
            System.out.println(totalRowCount);
            // System.out.println(table.size());
            // System.out.println(table.values()
            // .stream()
            // .mapToInt(IntList::size)
            // .sum());
            System.out.println(DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }

    /**
     * Test that reflects the 1Rbc challenge regarding the group by. This test fakes the CSV reading and only foces on HashAggregate performance.
     */
    @Disabled
    @Test
    void test_measure_1brc()
    {
        Random r = new Random(System.nanoTime());

        int rowCount = 1_000_000_000;
        int batchSize = 100_000;
        int batchCount = rowCount / batchSize;
        // final int[] numbers = new int[1_000_000_000];
        // for (int i = 0; i < numbers.length; i++)
        // {
        // numbers[i] = r.nextInt(100);
        // }

        final UTF8String[] strings = new UTF8String[413];
        for (int i = 0; i < strings.length; i++)
        {
            strings[i] = UTF8String.from(UUID.randomUUID()
                    .toString());
        }

        ValueVector city = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public boolean hasNulls()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return batchSize;
            }

            @Override
            public UTF8String getString(int row)
            {
                int actualRow = row % strings.length;
                return strings[actualRow];
            }
        };

        final float[] temps = new float[batchSize];
        for (int i = 0; i < temps.length; i++)
        {
            temps[i] = r.nextFloat();
        }

        ValueVector temp = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Float);
            }

            @Override
            public boolean hasNulls()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return batchSize;
            }

            @Override
            public float getFloat(int row)
            {
                return temps[row];
            }
        };

        Schema schema = schema(new Type[] { Type.String, Type.Float }, "city", "temp");

        IExpression cityExp = ce("city", 0, ResolvedType.STRING);
        IExpression tempExp = ce("temp", 1, ResolvedType.FLOAT);

        //@formatter:off
        IPhysicalPlan plan = new HashAggregate(1, new TableScan(0, schema, table, "", new IDatasource()
        {

            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return new TupleIterator()
                {
                    int batch = 0;
                    @Override
                    public TupleVector next()
                    {
                        batch++;
                        return TupleVector.of(schema, List.of(city, temp));
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return batch < batchCount;
                    }
                };
            }
        }, emptyList()), asList(cityExp), asList(
                new AggregateWrapperExpression(cityExp, true, false),
                new FunctionCallExpression("", SystemCatalog.get().getScalarFunction("min"), null, asList(tempExp)),
                new FunctionCallExpression("", SystemCatalog.get().getScalarFunction("max"), null, asList(tempExp))),
                null);
        //@formatter:on

        for (int i = 0; i < 10; i++)
        {
            // CSOFF
            long start = System.nanoTime();
            // CSON
            TupleIterator it = plan.execute(context);
            TupleVector actual = it.next();
            assertFalse(it.hasNext());
            it.close();

            System.out.println(actual.toCsv());

            ValueVector c = actual.getColumn(0);
            assertTrue(c.size() > 0);

            // System.out.println(actual.toCsv());
            System.out.println(DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }
}
