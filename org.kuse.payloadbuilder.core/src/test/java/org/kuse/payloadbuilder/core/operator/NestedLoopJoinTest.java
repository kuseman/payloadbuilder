package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
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
        RowIterator it = op.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(tableBPos[count], tuple.getTuple(2).getValue("__pos"));
            assertEquals(tableCPos[count], tuple.getTuple(3).getValue("__pos"));
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
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] tableAPos = new int[] {0, 0, 1, 1, 2, 2, 3, 3};
        int[] tableBPos = new int[] {0, 1, 0, 1, 0, 1, 0, 1};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(tableBPos[count], tuple.getTuple(1).getValue("__pos"));
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
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        RowIterator it = op.open(new ExecutionContext(session));

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(count, tuple.getTuple(0).getValue("__pos"));
            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);
            assertArrayEquals(new int[] {0, 1}, col.stream().mapToInt(t -> (int) t.getValue("__pos")).toArray());
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
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op(ctx -> new TableFunctionOperator(0, "", a, new Range(0), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(count, tuple.getTuple(0).getValue("__pos"));
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
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        RowIterator it = op.open(new ExecutionContext(session));

        Integer[] tableAPos = new Integer[] {0, 0, 1, 2, 2, 3};
        Integer[] tableBPos = new Integer[] {0, 1, null, 0, 1, null};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getTuple(0).getValue("__pos"));

            Integer val = Optional.ofNullable(tuple.getTuple(1)).map(t -> (Integer) t.getValue("__pos")).orElse(null);
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
                MapUtils.entry("tableA", a -> op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB", a -> op(ctx -> new TableFunctionOperator(0, "", a, new Range(2), emptyList()).open(ctx), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A nested loop should have been constructed", op instanceof NestedLoopJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getTuple(0).getValue("__pos");
            assertEquals(count, pos);
            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);
            if (pos % 2 == 0)
            {
                assertArrayEquals(new int[] {0, 1}, col.stream().mapToInt(t -> (int) t.getValue("__pos")).toArray());
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
        public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
        {
            if (tableAlias.getColumns() == null)
            {
                tableAlias.setColumns(new String[] {"Value"});
            }
            return RowIterator.wrap(IntStream.range(0, to).mapToObj(i -> (Tuple) Row.of(tableAlias, i, new Object[] {i + 1})).iterator());
        }
    }
}
