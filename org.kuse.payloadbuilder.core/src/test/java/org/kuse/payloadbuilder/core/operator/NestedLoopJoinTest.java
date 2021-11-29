package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test {@link NestedLoopJoin} */
public class NestedLoopJoinTest extends AOperatorTest
{
    @Test
    public void test_correlated()
    {
        // Test that a correlated query with a nestedloop
        // uses the context row into consideration when joining

        /**
         * <pre>
         * from tableA a
         * inner join
         * (
         *   tableB b
         *   inner join tableC c
         *      on c.id2 = a.id2
         *      or c.id = b.id
         * ) b
         *   on b.id = a.id
         * </pre>
         */

        String query = "select * from tableA a " +
            "inner join (" +
            "  select * " +
            "  from tableB b " +
            "  inner join tableC c " +
            "     on col2 = a.col2 " +
            "     or c.col1 = b.col1 " +
            ") b " +
            "  on b.col1 = a.col1";

        MutableInt aCloseCount = new MutableInt();
        MutableInt bCloseCount = new MutableInt();
        MutableInt cCloseCount = new MutableInt();

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA",
                        a -> op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> (Tuple) Row.of(a, i, new String[] {"col1", "col2"}, new Object[] {i, "val" + i})).iterator(),
                                () -> aCloseCount.increment())),
                MapUtils.entry("tableB",
                        a -> op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(a, 10 * i, new String[] {"col1", "col2"}, new Object[] {i})).iterator(), () -> bCloseCount.increment())),
                MapUtils.entry("tableC",
                        a -> op(ctx -> IntStream.of(1, 2, 3, 4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(a, 100 * i, new String[] {"col1", "col2"}, new Object[] {i, "val" + i})).iterator(),
                                () -> cCloseCount.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        int[] tableAPos = new int[] {4, 5};
        int[] tableBPos = new int[] {40, 50};
        int[] tableCPos = new int[] {400, 500};

        int count = 0;
        TupleIterator it = op.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], getValue(tuple, 0, Row.POS_ORDINAL));
            assertEquals(tableBPos[count], getValue(tuple, 2, Row.POS_ORDINAL));
            assertEquals(tableCPos[count], getValue(tuple, 3, Row.POS_ORDINAL));
            count++;
        }
        it.close();

        assertEquals(1, aCloseCount.intValue());
        // B is opened 5 times since there are 5 rows in A
        assertEquals(5, bCloseCount.intValue());
        // C is cached and hence only closed one time
        assertEquals(1, cCloseCount.intValue());
        assertEquals(2, count);
    }

    @Test
    public void test_cross_join_no_populate()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "cross apply tableB b";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] { "col" }, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op1(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        TupleIterator it = op.open(new ExecutionContext(session));

        int[] tableAPos = new int[] {0, 0, 1, 1, 2, 2, 3, 3};
        int[] tableBPos = new int[] {0, 1, 0, 1, 0, 1, 0, 1};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], getValue(tuple, -1, Row.POS_ORDINAL));
            assertEquals(tableBPos[count], getValue(tuple, 1, Row.POS_ORDINAL));
            count++;
        }
        it.close();
        assertFalse(it.hasNext());
        assertEquals(8, count);
        assertEquals(1, leftClose.intValue());
        assertEquals(1, rightClose.intValue());
    }

    @Test
    public void test_cross_join_populate()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "cross apply tableB b with (populate=true)";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] { "col" }, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op1(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        TupleIterator it = op.open(new ExecutionContext(session));

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(count, tuple.getValue(Row.POS_ORDINAL));
            @SuppressWarnings("unchecked")
            Iterable<Tuple> col = (Iterable<Tuple>) tuple.getTuple(1);
            assertArrayEquals(new int[] {0, 1}, stream(col).mapToInt(t -> (int) t.getValue(Row.POS_ORDINAL)).toArray());
            count++;
        }
        it.close();

        assertEquals(1, leftClose.intValue());
        assertEquals(1, rightClose.intValue());
        assertEquals(4, count);
    }

    @Test
    public void test_outer_join()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "outer apply tableB b ";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] { "col" }, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op1(ctx -> new TableFunctionOperator(0, "", a, new Range(0), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        TupleIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(count, tuple.getValue(Row.POS_ORDINAL));
            // Outer apply, no b rows should be present
            assertNull(tuple.getTuple(1));
            count++;
        }
        it.close();

        assertEquals(1, leftClose.intValue());
        assertEquals(1, rightClose.intValue());
        assertEquals(9, count);
    }

    @Test
    public void test_outer_join_with_predicate_no_populate()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "left join tableB b "
            + "  on a.__pos % 2 = 0 ";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] { "col" }, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op1(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        TupleIterator it = op.open(new ExecutionContext(session));

        Integer[] tableAPos = new Integer[] {0, 0, 1, 2, 2, 3};
        Integer[] tableBPos = new Integer[] {0, 1, null, 0, 1, null};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getValue(Row.POS_ORDINAL));

            Integer val = (Integer) getValue(tuple, 1, Row.POS_ORDINAL);
            assertEquals("Count: " + count, tableBPos[count], val);
            count++;
        }
        it.close();

        assertEquals(1, leftClose.intValue());
        assertEquals(1, rightClose.intValue());

        // 4 joined rows and 2 non joined
        assertEquals(6, count);
    }

    @Test
    public void test_outer_join_with_predicate_populate()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "left join tableB b with (populate=true) "
            + "  on a.__pos % 2 = 0 ";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] { "col" }, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op1(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        TupleIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(Row.POS_ORDINAL);
            assertEquals(count, pos);
            @SuppressWarnings("unchecked")
            Iterable<Tuple> col = (Iterable<Tuple>) tuple.getTuple(1);
            if (pos % 2 == 0)
            {
                assertArrayEquals(new int[] {0, 1}, stream(col).mapToInt(t -> (int) t.getValue(Row.POS_ORDINAL)).toArray());
            }
            else
            {
                assertNull(col);
            }

            count++;
        }
        it.close();

        assertEquals(1, leftClose.intValue());
        assertEquals(1, rightClose.intValue());
        assertEquals(4, count);
    }

    /** Test range function */
    static class Range extends TableFunctionInfo
    {
        private final int to;

        Range(int to)
        {
            super(new Catalog("test")
            {
            }, "Range");
            this.to = to;
        }

        @Override
        public TupleIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
        {
            return TupleIterator.wrap(IntStream.range(0, to).mapToObj(i -> (Tuple) Row.of(tableAlias, i, new String[] { "Value" }, new Object[] {i + 1})).iterator());
        }
    }
}
