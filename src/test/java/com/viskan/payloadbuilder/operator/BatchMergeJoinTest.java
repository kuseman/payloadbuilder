package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** Test {@link BatchMergeJoin} */
public class BatchMergeJoinTest extends Assert
{
    // TODO: add tests where predicate isn't always true

    @Test
    public void test_inner_join_empty_outer()
    {
        Operator left = context -> emptyIterator();
        Operator right = context -> emptyIterator();

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                -1);

        assertFalse(op.open(new OperatorContext()).hasNext());
    }

    @Test
    public void test_inner_join_empty_inner()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context -> emptyIterator();

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                -1);

        assertFalse(op.open(new OperatorContext()).hasNext());
    }

    @Test
    public void test_inner_join()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        Operator left = context -> IntStream.range(0, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            return rows.stream().filter(row -> (Integer) row.getObject(0) >= 5).map(row -> Row.of(b, (int) row.getObject(0), new Object[] {row.getObject(0), "Val" + row.getObject(0)})).iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                2);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertNull(row.getPredicateParent());
            assertNull(row.getChildRows(0).get(0).getPredicateParent());
            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            return rows.stream().filter(row -> (Integer) row.getObject(0) >= 5).map(row -> Row.of(b, (int) row.getObject(0), new Object[] {row.getObject(0), "Val" + row.getObject(0)})).iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                true,
                -1);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertNull(row.getPredicateParent());
            if (count < 4)
            {
                assertEquals(emptyList(), row.getChildRows(0));
            }
            else
            {
                assertNull(row.getChildRows(0).get(0).getPredicateParent());
                assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            }
            count++;
        }

        assertEquals(9, count);
    }

    @Test
    public void test_inner_join_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context -> context.getOuterRows()
                .stream()
                .filter(row -> (Integer) row.getObject(0) >= 5)
                .flatMap(row -> asList(new Object[] {row.getObject(0), 1}, new Object[] {row.getObject(0), 2}).stream())
                .map(ar -> Row.of(b, (int) ar[0], ar))
                .iterator();

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                2);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertNull(row.getPredicateParent());
            assertNull(row.getChildRows(0).get(0).getPredicateParent());
            assertNull(row.getChildRows(0).get(1).getPredicateParent());
            assertEquals(1, row.getChildRows(0).get(0).getObject(1));
            assertEquals(2, row.getChildRows(0).get(1).getObject(1));
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator right = context -> context.getOuterRows()
                .stream()
                .filter(row -> (Integer) row.getObject(0) >= 5)
                .flatMap(row -> asList(new Object[] {row.getObject(0), 1}, new Object[] {row.getObject(0), 2}).stream())
                .map(ar -> Row.of(b, (int) ar[0], ar))
                .iterator();

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                true,
                -1);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertNull(row.getPredicateParent());
            if (count < 4)
            {
                assertEquals(emptyList(), row.getChildRows(0));
            }
            else
            {
                assertNull(row.getChildRows(0).get(0).getPredicateParent());
                assertNull(row.getChildRows(0).get(1).getPredicateParent());
                assertEquals(1, row.getChildRows(0).get(0).getObject(1));
                assertEquals(2, row.getChildRows(0).get(1).getObject(1));
            }
            count++;
        }

        assertEquals(9, count);
    }

    @Test
    public void test_nested_joins_row_hierarchy_is_correct()
    {
        TableAlias a = TableAlias.of(null, "tableA", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        TableAlias c = TableAlias.of(b, "tableC", "c");

        Operator tableA = context -> IntStream.range(0, 2).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
        Operator tableB = context ->
        {
            List<Row> outerRows = context.getOuterRows();
            assertEquals(2, outerRows.size());
            assertEquals(0, outerRows.get(0).getObject(0));
            assertEquals(1, outerRows.get(1).getObject(0));
            return IntStream.range(0, 2).mapToObj(i -> Row.of(b, i, new Object[] {i, i + 10})).iterator();
        };
        Operator tableC = context ->
        {
            List<Row> outerRows = context.getOuterRows();
            assertEquals(2, outerRows.size());
            assertEquals(10, outerRows.get(0).getObject(1));
            assertEquals(11, outerRows.get(1).getObject(1));
            return IntStream.range(0, 2).mapToObj(i -> Row.of(c, i, new Object[] {i + 10})).iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                tableA,
                new BatchMergeJoin(
                        "",
                        tableB,
                        tableC,
                        (ctx, row) -> (Integer) row.getObject(1),
                        (ctx, row) -> (Integer) row.getObject(0),
                        (ctx, row) ->
                        {
                            assertSame(row.getTableAlias(), c);
                            assertSame(row.getParent().getTableAlias(), b);
                            assertSame(row.getParent().getParent().getTableAlias(), a);

                            return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(1);
                        },
                        DefaultRowMerger.DEFAULT,
                        false,
                        false,
                        250),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0),
                (ctx, row) ->
                {
                    assertSame(row.getTableAlias(), b);
                    assertSame(row.getParent().getTableAlias(), a);

                    return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
                },
                DefaultRowMerger.DEFAULT,
                false,
                false,
                250);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            it.next();
            count++;
        }

        assertEquals(2, count);
    }

    @Ignore
    @Test
    public void test_inner_join_large()
    {
        /*
         *
         *
         *
         */

        Random rnd = new Random();
        TableAlias a = TableAlias.of(null, "tableA", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        TableAlias c = TableAlias.of(b, "tableC", "c");
        Operator tableA = context -> IntStream.range(1, 100000).mapToObj(i -> Row.of(a, i, new Object[] {i, rnd.nextBoolean()})).iterator();
        Operator tableB = context ->
        {
            return context.getOuterRows()
                    .stream()
                    .flatMap(row -> asList(
                            new Object[] {row.getObject(0), 1, rnd.nextBoolean()},
                            new Object[] {row.getObject(0), 2, rnd.nextBoolean()},
                            new Object[] {row.getObject(0), 3, rnd.nextBoolean()},
                            new Object[] {row.getObject(0), 4, rnd.nextBoolean()}).stream())
                    .map(ar2 -> Row.of(b, (int) ar2[0], ar2))
                    .iterator();
        };
        Operator tableC = context ->
        {
            return context.getOuterRows()
                    .stream()
                    .flatMap(row -> asList(
                            new Object[] {row.getObject(0), 1, "Val" + row.getObject(0)},
                            new Object[] {row.getObject(0), 4, rnd.nextBoolean()}).stream())
                    .map(ar -> Row.of(c, (int) ar[0], ar))
                    .iterator();
        };

        /**
         * from tableA a inner join [ tableB b inner join [tableC] c on c.id = b.id ] b on b.id = a.id
         */

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                tableA,
                new BatchMergeJoin(
                        "",
                        tableB,
                        tableC,
                        (ctx, row) -> (int) row.getObject(0),
                        (ctx, row) -> (int) row.getObject(0),
                        (ctx, row) ->
                        {
                            return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
                        },
                        DefaultRowMerger.DEFAULT,
                        false,
                        false,
                        250),
                (ctx, row) -> (int) row.getObject(0),
                (ctx, row) -> (int) row.getObject(0),
                (ctx, row) ->
                {
                    return (Boolean) row.getObject(2)
                        && (Boolean) row.getParent().getObject(1)
                        && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
                },
                DefaultRowMerger.DEFAULT,
                true,
                false,
                250);

        for (int i = 0; i < 150; i++)
        {
            StopWatch sw = new StopWatch();
            sw.start();
            Iterator<Row> it = op.open(new OperatorContext());
            int count = 0;
            while (it.hasNext())
            {
                Row row = it.next();
                //                assertEquals(4, row.getChildRows(0).size());
                //                            System.out.println(row);
                //            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
                count++;
            }
            sw.stop();
            long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            System.out.println("Time: " + sw.toString() + " rows: " + count + " mem: " + FileUtils.byteCountToDisplaySize(mem));
        }
        //        assertEquals(5, count);
    }
}
