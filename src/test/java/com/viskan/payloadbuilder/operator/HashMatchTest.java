package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/** Test {@link HashMatch} */
public class HashMatchTest extends Assert
{
    @Test
    public void test_inner_join_no_populate_empty()
    {
        HashMatch op = new HashMatch(
                "",
                c -> emptyIterator(),
                c -> emptyIterator(),
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParents().get(0).getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false);
        Iterator<Row> it = op.open(new OperatorContext());
        assertFalse(it.hasNext());
    }
        
    @Test
    public void test_inner_join_no_populate()
    {
        TableAlias a = TableAlias.of(null, "table", "t");
        TableAlias r = TableAlias.of(a, "range", "r");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator();
        HashMatch op = new HashMatch(
                "",
                left,
                new TableFunctionOperator(r, new NestedLoopTest.Range(5), emptyList()),
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParents().get(0).getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false);

        Iterator<Row> it = op.open(new OperatorContext());
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
    public void test_inner_join_populate()
    {
        TableAlias t = TableAlias.of(null, "table", "t");
        TableAlias t2 = TableAlias.of(t, "table2", "t2");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(t, i - 1, new Object[] {i})).iterator();
        Operator right = context -> IntStream.range(1, 20).mapToObj(i -> Row.of(t2, i - 1, new Object[] {i % 10})).iterator();
        HashMatch op = new HashMatch(
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParents().get(0).getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false);

        Iterator<Row> it = op.open(new OperatorContext());
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
    public void test_inner_join_populate_2()
    {
        TableAlias t = TableAlias.of(null, "table", "t");
        TableAlias t2 = TableAlias.of(t, "table2", "t2");
        Operator left = context -> IntStream.range(1, 20).mapToObj(i -> Row.of(t2, i - 1, new Object[] {i % 10})).iterator();
        Operator right = context -> IntStream.range(1, 7).mapToObj(i -> Row.of(t, i - 1, new Object[] {i})).iterator();
        HashMatch op = new HashMatch(
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParents().get(0).getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals(row.getObject(0), row.getChildRows(0).get(0).getObject(0));
            count++;
        }
        assertFalse(it.hasNext());
        assertEquals(12, count);
    }

    @Test
    public void test_outer_join_no_populate()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "table", "b");
        Operator left = context -> IntStream.range(0, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context -> IntStream.range(5, 15).mapToObj(i -> Row.of(b, i - 1, new Object[] {i})).iterator();
        
        HashMatch op = new HashMatch(
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParents().get(0).getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                true);
        
        Iterator<Row> it = op.open(new OperatorContext());
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
        Operator left = context -> IntStream.range(0, 5).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context -> IntStream.range(5, 15).mapToObj(i -> Row.of(b, i - 1, new Object[] {i % 5 + 2})).iterator();
        
        HashMatch op = new HashMatch(
                "",
                left,
                right,
                (c, row) -> (Integer) row.getObject(0),
                (c, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0) == (int) row.getParents().get(0).getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                true);
        
        Iterator<Row> it = op.open(new OperatorContext());
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
