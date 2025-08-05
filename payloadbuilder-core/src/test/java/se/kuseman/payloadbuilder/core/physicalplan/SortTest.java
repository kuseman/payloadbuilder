package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.junit.Ignore;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.QueryException;

/** Test of {@link Sort} */
public class SortTest extends APhysicalPlanTest
{
    @Test
    public void test_no_rows()
    {
        IPhysicalPlan sort = new Sort(1, scan(schemaLessDS(() ->
        {
        }, new TupleVector[0]), table, Schema.EMPTY), asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED), sortItem(ce("col2"), Order.ASC, NullOrder.UNDEFINED)));
        TupleIterator it = sort.execute(context);
        assertFalse(it.hasNext());
    }

    @Test
    public void test_nulls_undefined_schema_less()
    {
        Schema schema = schema(new Type[] { Type.Int, Type.Any }, "col1", "col2");

        MutableBoolean closed = new MutableBoolean();
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Int, null, 20, 10, 30, 20, null), vv(Type.Any, -1, 0, 1, 2, 3, 4)))), table, Schema.EMPTY),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED), sortItem(ce("col2"), Order.DESC, NullOrder.UNDEFINED)));

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // CSOFF
        TupleVector actual = it.next();
        // CSON
        assertFalse(it.hasNext());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }

        it.close();

        assertEquals(2, actual.getSchema()
                .getSize());

        assertVectorsEquals(vv(Type.Int, null, null, 10, 20, 20, 30), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, -1, 1, 3, 0, 2), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_nulls_first_schema_less()
    {
        Schema schema = schema(new Type[] { Type.Long, Type.Any }, "col1", "col2");

        MutableBoolean closed = new MutableBoolean();
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Long, null, 20L, 10L, 30L, 20L, null), vv(Type.Any, -1, 0, 1, 2, 3, 4)))), table, Schema.EMPTY),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.FIRST), sortItem(ce("col2"), Order.DESC, NullOrder.UNDEFINED)));

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // There is now only one vector returned
        TupleVector actual = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(Schema.of(col("col1", ResolvedType.of(Type.Long), table), col("col2", ResolvedType.of(Type.Any), table)), actual.getSchema());

        assertVectorsEquals(vv(Type.Long, null, null, 10L, 20L, 20L, 30L), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, -1, 1, 3, 0, 2), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_nulls_last_schema_less()
    {
        Schema schema = schema(new Type[] { Type.Float, Type.Any }, "col1", "col2");

        MutableBoolean closed = new MutableBoolean();
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Float, null, 20F, 10F, 30F, 20F, null), vv(Type.Any, -1, 0, 1, 2, 3, 4)))), table, Schema.EMPTY),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.LAST), sortItem(ce("col2"), Order.DESC, NullOrder.UNDEFINED)));

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // There is now only one vector returned
        TupleVector actual = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(2, actual.getSchema()
                .getSize());

        assertVectorsEquals(vv(Type.Float, 10F, 20F, 20F, 30F, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 3, 0, 2, 4, -1), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_multiple_vectors_schema_less()
    {
        Schema schema = schema(new Type[] { Type.Double, Type.Any }, "col1", "col2");
        MutableBoolean closed = new MutableBoolean();
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Double, 20D, 10D), vv(Type.Any, 1, 2))),
                        TupleVector.of(schema, asList(vv(Type.Double, 30D, 20D), vv(Type.Any, 3, 4)))), table, Schema.EMPTY),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED), sortItem(ce("col2"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // There is now only one vector returned
        TupleVector actual = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(4, actual.getRowCount());

        assertVectorsEquals(vv(Type.Double, 10D, 20D, 20D, 30D), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 2, 1, 4, 3), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_boolean()
    {
        Schema schema = schema(new Type[] { Type.Boolean, Type.Any }, "col1", "col2");
        MutableBoolean closed = new MutableBoolean();
        IPhysicalPlan sort = new Sort(1, scan(schemaLessDS(() -> closed.setTrue(), TupleVector.of(schema, asList(vv(Type.Boolean, true, false), vv(Type.Any, 1, 2)))), table, Schema.EMPTY),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // There is now only one vector returned
        TupleVector actual = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.Boolean, false, true), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 2, 1), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_string()
    {
        Schema schema = schema(new Type[] { Type.String, Type.Any }, "col1", "col2");
        MutableBoolean closed = new MutableBoolean();
        //@formatter:off
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), 
                        TupleVector.of(schema, asList(vv(Type.String, "abcåäö", "åäöabc"), vv(Type.Any, 1, 2)))), table, Schema.EMPTY),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // There is now only one vector returned
        TupleVector actual = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.String, UTF8String.from("abcåäö"), UTF8String.from("åäöabc")), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_ordinal()
    {
        Schema schema = schema(new Type[] { Type.String, Type.Any }, "col1", "col2");
        MutableBoolean closed = new MutableBoolean();
        //@formatter:off
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), 
                        TupleVector.of(schema, asList(vv(Type.String, "abcåäö", "åäöabc"), vv(Type.Any, 1, 2)))), table, Schema.EMPTY),
                asList(sortItem(intLit(1), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        assertEquals(Schema.EMPTY, sort.getSchema());

        TupleIterator it = sort.execute(context);
        // There is now only one vector returned
        TupleVector actual = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.String, UTF8String.from("abcåäö"), UTF8String.from("åäöabc")), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        assertTrue(closed.get());
    }

    @Test
    public void test_ordinal_out_of_range()
    {
        Schema schema = schema(new Type[] { Type.String, Type.Any }, "col1", "col2");

        MutableBoolean closed = new MutableBoolean();
        //@formatter:off
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), 
                        TupleVector.of(schema, asList(vv(Type.String, "abcåäö", "åäöabc"), vv(Type.Any, 1, 2)))), table, Schema.EMPTY),
                asList(sortItem(intLit(0), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        try
        {
            PlanUtils.concat(context, sort.execute(context));
            fail("Should fail with out of range");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("ORDER BY position is out of range"));
        }
    }

    @Test
    public void test_ordinal_out_of_range_2()
    {
        Schema schema = schema(new Type[] { Type.String, Type.Any }, "col1", "col2");

        MutableBoolean closed = new MutableBoolean();
        //@formatter:off
        IPhysicalPlan sort = new Sort(1,
                scan(schemaLessDS(() -> closed.setTrue(), 
                        TupleVector.of(schema, asList(vv(Type.String, "abcåäö", "åäöabc"), vv(Type.Any, 1, 2)))), table, Schema.EMPTY),
                asList(sortItem(intLit(10), Order.ASC, NullOrder.UNDEFINED)));
        //@formatter:on

        try
        {
            PlanUtils.concat(context, sort.execute(context));
            fail("Should fail with out of range");
        }
        catch (QueryException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("ORDER BY position is out of range"));
        }
    }

    @Ignore
    @Test
    public void test_measure()
    {
        Random r = new Random(System.nanoTime());
        int size = 5_000_000;
        final int[] numbers = new int[size];
        List<UTF8String> bytes = new ArrayList<>(size);
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < numbers.length; i++)
        {
            numbers[i] = r.nextInt();
            bytes.add(UTF8String.from("string" + numbers[i]));
            // strings.add("string" + numbers[i]);
        }

        ValueVector vv = new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                // Test sort primitive vs instance
                return ResolvedType.of(Type.String);
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
                return strings.get(row);
            }

            @Override
            public UTF8String getString(int row)
            {
                return bytes.get(row);
            }

            @Override
            public int getInt(int row)
            {
                return numbers[row];
            }
        };

        Schema schema = schema(new Type[] { Type.String }, "col1");
        IPhysicalPlan sort = new Sort(1, scan(schemaLessDS(() ->
        {
        }, TupleVector.of(schema, asList(vv))), table, schema), asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        for (int i = 0; i < 100; i++)
        {
            // CSOFF
            long start = System.nanoTime();
            // CSON
            TupleIterator it = sort.execute(context);
            TupleVector actual = it.next();
            assertFalse(it.hasNext());
            it.close();

            ValueVector c = actual.getColumn(0);

            assertTrue(c.size() > 0);
            // assertTrue( c.getInt(0) < c.getInt(10));

            // System.out.println(actual.toCsv());
            System.out.println(DurationFormatUtils.formatDurationHMS(TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS)));
        }
    }
}
