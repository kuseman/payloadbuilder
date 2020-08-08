package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

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
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false);
        Iterator<Row> it = op.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }
        
    @Test
    public void test_inner_join_no_populate()
    {
        TableAlias a = TableAlias.of(null, "table", "t");
        TableAlias r = TableAlias.of(a, "range", "r");
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator());
        HashJoin op = new HashJoin(0,
                "",
                left,
                new TableFunctionOperator(0, r, new NestedLoopJoinTest.Range(5), emptyList()),
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false);

        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals(count + 1, row.getChildRows(0).get(0).getObject(0));
            count++;
        }
        assertFalse(it.hasNext());
        assertEquals(5, count);
    }
    
    @Test
    public void test_correlated()
    {
        // Test that a correlated query with a batch hash join
        // uses the context row into consideration when joining

        TableAlias a = TableAlias.of(null, "tableA", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        TableAlias c = TableAlias.of(b, "tableC", "c");

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

        Operator opA = op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> Row.of(a, i, new Object[] {i, "val" + i})).iterator());
        Operator opB = op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> Row.of(b, i, new Object[] {i})).iterator());
        Operator opC = op(ctx -> IntStream.of(1, 2, 3, 4, 5, 6, 7).mapToObj(i -> Row.of(c, i, new Object[] {i, "val" + i})).iterator());

        Operator op = new NestedLoopJoin(
                0,
                "",
                opA,
                new HashJoin(
                        1,
                        "",
                        opB,
                        opC,
                        (ctx, row) -> (int) row.getObject(0),
                        (ctx, row) -> (int) row.getObject(0),
                        (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0)
                            && Objects.equals(row.getObject(1), row.getParent().getParent().getObject(1)),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false);

        int[] tableAPos = new int[] {4, 5};
        int[] tableBPos = new int[] {4, 5};
        int[] tableCPos = new int[] {4, 5};

        int count = 0;
        Iterator<Row> it = op.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals(row.getPos(), tableAPos[count]);
            assertEquals(row.getChildRows(0).get(0).getPos(), tableBPos[count]);
            assertEquals(row.getChildRows(0).get(0).getChildRows(0).get(0).getPos(), tableCPos[count]);

            count++;
        }
    }
    
    @Test
    public void test_inner_join_populate()
    {
        TableAlias t = TableAlias.of(null, "table", "t");
        TableAlias t2 = TableAlias.of(t, "table2", "t2");
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(t, i - 1, new Object[] {i})).iterator());
        Operator right = op(context -> IntStream.range(1, 20).mapToObj(i -> Row.of(t2, i - 1, new Object[] {i % 10})).iterator());
        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false);

        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals(count, row.getPos());
            assertEquals(count, row.getChildRows(0).get(0).getPos());
            assertEquals(count + 10, row.getChildRows(0).get(1).getPos());
            assertEquals(count + 1, row.getObject(0));
            assertEquals(count + 1, row.getChildRows(0).get(0).getObject(0));
            assertEquals(count + 1, row.getChildRows(0).get(1).getObject(0));
            count++;
        }
        assertFalse(it.hasNext());

        assertEquals(9, count);
    }
    
    @Test
    public void test_outer_join_no_populate()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "table", "b");
        Operator left = op(context -> IntStream.range(0, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context -> IntStream.range(5, 15).mapToObj(i -> Row.of(b, i - 1, new Object[] {i})).iterator());
        
        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                true);
        
        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            if (row.getPos() >= 5)
            {
                assertEquals(row.getObject(0), row.getChildRows(0).get(0).getObject(0));
            }
            else
            {
                assertEquals(emptyList(), row.getChildRows(0));
            }
            count++;
        }

        assertEquals(10, count);
    }
    
    @Test
    public void test_outer_join_populate()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "table", "b");
        Operator left = op(context -> IntStream.range(0, 5).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context -> IntStream.range(5, 15).mapToObj(i -> Row.of(b, i - 1, new Object[] {i % 5 + 2})).iterator());
        
        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                true);
        
        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            if (row.getPos() >= 2)
            {
                assertEquals(row.getObject(0), row.getChildRows(0).get(0).getObject(0));
                assertEquals(row.getObject(0), row.getChildRows(0).get(1).getObject(0));
            }
            else
            {
                assertEquals(emptyList(), row.getChildRows(0));
            }
            count++;
        }

        assertEquals(5, count);
    }
}