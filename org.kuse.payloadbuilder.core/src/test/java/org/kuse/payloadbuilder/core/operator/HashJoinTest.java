package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Test {@link HashJoin} */
public class HashJoinTest extends AOperatorTest
{
    @Test
    public void test_inner_join_no_populate_empty()
    {
        HashJoin op = new HashJoin(0,
                "",
                op(c -> emptyIterator()),
                op(c -> emptyIterator()),
                (c, tuple) -> 0,
                (c, tuple) -> 0,
                ctx -> false,
                new DefaultTupleMerger(-1, 1, 2),
                false,
                false);
        TupleIterator it = op.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_no_populate()
    {
        MutableInt leftClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "inner join range(1, 6) b "
            + "  on b.Value = a.col1";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA",
                        a -> op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] {"col1"}, new Object[] {i})).iterator(), () -> leftClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A hash join should have been constructed", op instanceof HashJoin);

        TupleIterator it = op.open(new ExecutionContext(session));

        int[] tableAPos = new int[] {1, 2, 3, 4, 5};
        int[] tableBPos = new int[] {1, 2, 3, 4, 5};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(tableAPos[count], getValue(tuple, 0, "col1"));
            assertEquals(tableBPos[count], getValue(tuple, 1, "Value"));
            count++;
        }
        it.close();
        assertFalse(it.hasNext());
        assertEquals(5, count);
        assertEquals(1, leftClose.intValue());
    }

    @Test
    public void test_correlated()
    {
        // Test that a correlated query with a batch hash join
        // uses the context row into consideration when joining

        /**
         * <pre>
         * from tableA a
         * inner join
         * [
         *   tableB b
         *   inner join [tableC] c
         *      on c.id = b.id
         *      and c.id2 = a.id2
         * ] b
         *   on b.id = a.id
         * </pre>
         */

        String query = "select * from tableA a " +
            "inner join (" +
            "  select * " +
            "  from tableB b " +
            "  inner join tableC c " +
            "     on col2 = a.col2 " +
            "     and c.col1 = b.col1 " +
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

        assertTrue("A Nested loop should have been constructed", op instanceof NestedLoopJoin);
        assertTrue("A Hash join should have been constructed as inner operator", ((NestedLoopJoin) op).getInner() instanceof HashJoin);

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
        assertEquals(5, cCloseCount.intValue());

        assertEquals(2, count);
    }

    @Test
    public void test_inner_join_populate()
    {
        MutableInt leftClose = new MutableInt();

        String query = "select * "
            + "from range(1, 10) a "
            + "inner join tableB b with (populate=true) "
            + "  on b.Value = a.Value";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableB",
                        a -> op(context -> IntStream.range(1, 20).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] {"Value"}, new Object[] {i % 10})).iterator(), () -> leftClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A hash join should have been constructed", op instanceof HashJoin);

        TupleIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(count, tuple.getInt(0) - 1);

            @SuppressWarnings("unchecked")
            Iterable<Tuple> col = (Iterable<Tuple>) tuple.getTuple(1);
            assertArrayEquals(new int[] {count, count + 10}, stream(col).mapToInt(t -> (int) t.getValue(Row.POS_ORDINAL)).toArray());
            count++;
        }
        it.close();
        assertFalse(it.hasNext());
        assertEquals(9, count);
        assertEquals(1, leftClose.intValue());
    }

    @Test
    public void test_outer_join_no_populate()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "left join tableB b "
            + "  on b.Value = a.Value";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA",
                        a -> op(context -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(a, i, new String[] {"Value"}, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB",
                        a -> op(context -> IntStream.range(5, 15).mapToObj(i -> (Tuple) Row.of(a, i, new String[] {"Value"}, new Object[] {i})).iterator(), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A hash join should have been constructed", op instanceof HashJoin);

        TupleIterator it = op.open(new ExecutionContext(session));

        Integer[] tableAPos = new Integer[] {5, 6, 7, 8, 9, 0, 1, 2, 3, 4};
        Integer[] tableBPos = new Integer[] {5, 6, 7, 8, 9, null, null, null, null, null};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getValue(Row.POS_ORDINAL));

            Integer val = (Integer) getValue(tuple, 1, Row.POS_ORDINAL);
            assertEquals(tableBPos[count], val);
            count++;
        }
        it.close();

        assertEquals(10, count);
        assertEquals(1, leftClose.intValue());
        assertEquals(1, rightClose.intValue());
    }

    @Test
    public void test_outer_join_populate()
    {
        MutableInt leftClose = new MutableInt();
        MutableInt rightClose = new MutableInt();

        String query = "select * "
            + "from tableA a "
            + "left join tableB b with (populate = true) "
            + "  on b.Value = a.Value";

        Map<String, Function<TableAlias, Operator>> opByTable = MapUtils.ofEntries(
                MapUtils.entry("tableA",
                        a -> op(context -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(a, i, new String[] {"Value"}, new Object[] {i})).iterator(), () -> leftClose.increment())),
                MapUtils.entry("tableB",
                        a -> op(context -> IntStream.range(5, 15).mapToObj(i -> (Tuple) Row.of(a, i - 1, new String[] {"Value"}, new Object[] {i % 5 + 2})).iterator(), () -> rightClose.increment())));

        Operator op = operator(query, opByTable);

        assertTrue("A hash join should have been constructed", op instanceof HashJoin);

        TupleIterator it = op.open(new ExecutionContext(session));

        int[][] tableBPos = new int[][] {
                new int[] {4, 9},
                new int[] {5, 10},
                new int[] {6, 11},
                new int[] {7, 12},
                new int[] {8, 13}
        };

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(Row.POS_ORDINAL);
            assertEquals(count, pos);

            @SuppressWarnings("unchecked")
            Iterable<Tuple> col = (Iterable<Tuple>) tuple.getTuple(1);

            if (pos >= 2 && pos <= 6)
            {
                assertArrayEquals("Count: " + count, tableBPos[pos - 2], stream(col).mapToInt(t -> (int) t.getValue(Row.POS_ORDINAL)).toArray());
            }
            else
            {
                assertNull("Count: " + count, col);
            }
            count++;
        }

        assertEquals(10, count);
    }
}
