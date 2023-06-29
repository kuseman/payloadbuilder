package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.junit.Ignore;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.ArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;

/** Test of {@link HashAggregate} */
public class HashAggregateTest extends APhysicalPlanTest
{
    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_input()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");
        IDatasource ds = schemaLessDS(() ->
        {
        });

        new HashAggregate(0, scan(ds, table, schema), asList(ce("col1")), emptyList());
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_input_2()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");
        IDatasource ds = schemaLessDS(() ->
        {
        });

        new HashAggregate(0, scan(ds, table, schema), emptyList(), asList(new AggregateWrapperExpression(ce("col1"), false, false)));
    }

    @Test
    public void test_distinct()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");

        IDatasource ds = schemaLessDS(() ->
        {
        }, TupleVector.of(schema, asList(vv(Type.Int, 1, null, 2, 3, 3, 3, 4, 4, 4, 4, null), vv(Type.Any, 1, 2, 2, 3, 3, 6, 4, 4, 9, 10, null))));

        //@formatter:off
        IPhysicalPlan plan =
                new Sort(2,
                        new HashAggregate(1, scan(ds, table, Schema.EMPTY), emptyList(), emptyList()),
                asList(sortItem(ce("col1", 0), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        // Plan schema
        assertEquals(Schema.EMPTY, plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), it);

        // Runtime schema
        //@formatter:off
        assertEquals(Schema.of(
                new CoreColumn("col1", ResolvedType.of(Type.Int), table.column("col1")),
                new CoreColumn("col2", ResolvedType.of(Type.Any), table.column("col2"))), actual.getSchema());
        //@formatter:on

        //@formatter:off
        assertVectorsEquals(vv(Type.Int, null, null, 1, 2, 3, 3, 4, 4,  4), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, null, 2,    1, 2, 3, 6, 9, 10, 4), actual.getColumn(1));
        //@formatter:on
    }

    @Test
    public void test_schema_less()
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
                                    new AggregateWrapperExpression(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, col1, col2), false, false))),
                asList(sortItem(ce("count", 0), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        // Plan schema
        //@formatter:off
        assertEquals(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Int), "count(col1)", false),
                new CoreColumn("col2", ResolvedType.array(ResolvedType.of(Type.Any)), "", false),
                new CoreColumn("", ResolvedType.of(Type.Int), "sum(col1)", false),
                new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Int)), "col1 + col2", false)),
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
                new CoreColumn("", ResolvedType.of(Type.Int), "count(col1)", false),
                new CoreColumn("col2", ResolvedType.array(ResolvedType.of(Type.Any)), "", false),
                new CoreColumn("", ResolvedType.of(Type.Int), "sum(col1)", false),
                new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Int)), "col1 + col2", false)),
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

    @Ignore
    @Test
    public void test_measure()
    {
        Random r = new Random(System.nanoTime());
        final int[] numbers = new int[10_000_000];
        for (int i = 0; i < numbers.length; i++)
        {
            numbers[i] = r.nextInt(100);
        }

        ValueVector vv = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                // Test grouping primitive vs instance
                return ResolvedType.of(Type.Int);
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int size()
            {
                return numbers.length;
            }

            @Override
            public Object getAny(int row)
            {
                return numbers[row];
            }

            @Override
            public int getInt(int row)
            {
                return numbers[row];
            }
        };

        Schema schema = schema(new Type[] { Type.Int }, "col1");

        IDatasource ds = schemaLessDS(() ->
        {
        }, TupleVector.of(schema, asList(vv)));

        IExpression col1 = ce("col1");

        IPhysicalPlan plan = new HashAggregate(1, scan(ds, table, schema), asList(col1), asList(new FunctionCallExpression("", SystemCatalog.get()
                .getScalarFunction("count"), null, asList(col1)), new FunctionCallExpression("",
                        SystemCatalog.get()
                                .getScalarFunction("sum"),
                        null, asList(col1))));

        for (int i = 0; i < 1; i++)
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
