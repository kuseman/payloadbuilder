package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Assert;
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
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
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
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                -1);

        assertFalse(op.open(new OperatorContext()).hasNext());
    }

    @Test
    public void test_inner_join_one_to_one()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                7,          // Batch 1
                8, 9, 10,   // Batch 2
                11          // Batch 3
        };

        int[] expectedInnerPositions = new int[] {
                0,          // Batch 1
                1, 2, 3,    // Batch 2
                4           // Batch 3
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //            System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count], row.getChildRows(0).get(0).getPos());
            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_one()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                true,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,    // Batch 1
                3, 4, 5,    // Batch 2
                6, 7, 8,    // Batch 3
                9, 10, 11,  // Batch 3
        };

        int[] expectedInnerPositions = new int[] {
                0,          // Batch 1
                1, 2, 3,    // Batch 2
                4           // Batch 3
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                        System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[count - 5], row.getChildRows(0).get(0).getPos());
                assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }
            count++;
        }

        assertEquals(12, count);
    }

    @Test
    public void test_inner_join_one_to_many()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> IntStream.range(-2, 12).mapToObj(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            return rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                2);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                7, 7, 8, 8,   // Batch 1
                9, 9, 10, 10, // Batch 2
                11, 11        // Batch 3
        };

        int[] expectedInnerPositions = new int[] {
                0, 1, 2, 3, // Batch 1
                4, 5, 6, 7, // Batch 2
                8, 9        // Batch 3
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //            System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count], row.getChildRows(0).get(0).getPos());
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_one_to_many()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> IntStream.range(-2, 12).mapToObj(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            return rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                true,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,                // Batch 1
                3, 4, 5,                // Batch 2
                6, 7, 7, 8, 8,          // Batch 3
                9, 9, 10, 10, 11, 11,   // Batch 4
                12, 13                  // Batch 5
        };

        int[] expectedInnerPositions = new int[] {
                0, 1, 2, 3,         // Batch 1
                4, 5, 6, 7, 8, 9    // Batch 2
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //            System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[count - 7], row.getChildRows(0).get(0).getPos());

            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }
            count++;
        }

        assertEquals(19, count);
    }

    @Test
    public void test_inner_join_many_to_one()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                5);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 14,         // Batch 1
                15, 16, 17, 18, 19, // Batch 2
                20, 21              // Batch 3
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1,        // Batch 1
                2, 3, 3, 4, 4,  // Batch 2      
                5, 5            // Batch 2
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                                    System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count], row.getChildRows(0).get(0).getPos());
            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_many_to_one()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                true,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
                12, 13, 14,
                15, 16, 17,
                18, 19, 20,
                21, 22, 23
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1,        // Batch 1
                2, 3, 3,        // Batch 2      
                4, 4, 5,        // Batch 2
                6
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //            System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[count - 12], row.getChildRows(0).get(0).getPos());
            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }
            count++;
        }

        assertEquals(24, count);
    }

    @Test
    public void test_inner_join_many_to_many()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            // Create 2 rows for each input row
            List<Row> rows = new ArrayList<>(context.getOuterRows());
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 12, 13, 14, 14,     // Batch 1
                15, 15, 16, 17, 16, 17,     // Batch 2
                18, 19, 18, 19, 20, 20,     // Batch 3
                21, 21                      // Batch 4
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1, 1, 2, 3,       // Batch 1
                4, 5, 6, 6, 7, 7,       // Batch 2
                8, 8, 9, 9, 10, 11,     // Batch 3
                12, 13                  // Batch 4
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                        System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count], row.getChildRows(0).get(0).getPos());
            count++;
        }

        assertEquals(20, count);
    }

    @Test
    public void test_outer_join_many_to_many()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            // Create 2 rows for each input row
            List<Row> rows = new ArrayList<>(context.getOuterRows());
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}, new Object[] {val, 3}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0
                    && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0)
                    && (Integer) row.getObject(1) < 3,
                DefaultRowMerger.DEFAULT,
                false,
                true,
                3);

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
                12, 13, 12, 13, 14, 14,
                15, 15, 16, 17, 16, 17,
                18, 19, 18, 19, 20, 20,
                21, 21, 22, 23
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1, 1, 3, 4,
                6, 7, 9, 9, 10, 10,
                12, 12, 13, 13, 15, 16,
                18, 19
        };

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                                    System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[count - 12], row.getChildRows(0).get(0).getPos());
            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }

            count++;
        }

        assertEquals(34, count);
    }

    @Test
    public void test_inner_join_one_to_one_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                7, 8,       // Batch 1
                9, 10, 11   // Batch 2
        };

        int[] expectedInnerPositions = new int[] {
                0, 1,       // Batch 1
                2, 3, 4     // Batch 2
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                                    System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count], row.getChildRows(0).get(0).getPos());
            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_one_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                true,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
                12, 13
        };

        int[] expectedInnerPositions = new int[] {
                0, 1,
                2, 3, 4
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                                                System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[count - 7], row.getChildRows(0).get(0).getPos());
                assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }
            count++;
        }

        assertEquals(14, count);
    }

    @Test
    public void test_inner_join_one_to_many_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> IntStream.range(-2, 12).mapToObj(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            return rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                2);

        Iterator<Row> it = op.open(new OperatorContext());

        int[] expectedOuterPositions = new int[] {
                7,
                8, 9,
                10, 11
        };

        int[] expectedInnerPositions = new int[] {
                0, 1,       // Batch 1
                2, 3, 4, 5, // Batch 2
                6, 7, 8, 9  // Batch 3
        };

        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                                    System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count * 2], row.getChildRows(0).get(0).getPos());
            assertEquals(expectedInnerPositions[(count * 2) + 1], row.getChildRows(0).get(1).getPos());
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_many_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> IntStream.range(-2, 12).mapToObj(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator();
        Operator right = context ->
        {
            List<Row> rows = context.getOuterRows();
            return rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                true,
                2);

        Iterator<Row> it = op.open(new OperatorContext());

        int[] expectedOuterPositions = new int[] {
                0, 1,
                2, 3,
                4, 5,
                6, 7,
                8, 9,
                10, 11,
                12, 13
        };

        int[] expectedInnerPositions = new int[] {
                0, 1,
                2, 3, 4, 5,
                6, 7, 8, 9
        };

        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //            System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[(count - 7) * 2], row.getChildRows(0).get(0).getPos());
                assertEquals(expectedInnerPositions[((count - 7) * 2) + 1], row.getChildRows(0).get(1).getPos());
            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }
            count++;
        }

        assertEquals(14, count);
    }

    @Test
    public void test_inner_join_many_to_many_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            // Create 2 rows for each input row
            List<Row> rows = new ArrayList<>(context.getOuterRows());
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 14,     // Batch 1
                15, 16, 17,     // Batch 2
                18, 19, 20,     // Batch 3
                21,             // Batch 4
        };

        int[] expectedInnerPositions = new int[] {
                0, 1, 0, 1, 2, 3,       // Batch 1
                4, 5, 6, 7, 6, 7,       // Batch 2
                8, 9, 8, 9, 10, 11,     // Batch 3
                12, 13                  // Batch 4
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
            //                        System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            assertEquals(expectedInnerPositions[count * 2], row.getChildRows(0).get(0).getPos());
            assertEquals(expectedInnerPositions[(count * 2) + 1], row.getChildRows(0).get(1).getPos());
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_many_to_many_populating()
    {
        TableAlias a = TableAlias.of(null, "table", "a");
        TableAlias b = TableAlias.of(a, "tableB", "b");
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator();
        Operator right = context ->
        {
            // Create 2 rows for each input row
            List<Row> rows = new ArrayList<>(context.getOuterRows());
            List<Row> inner = rows
                    .stream()
                    .map(row -> (Integer) row.getObject(0))
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .collect(toList());

            return inner.iterator();
        };

        BatchMergeJoin op = new BatchMergeJoin(
                "",
                left,
                right,
                (ctx, rowA, rowB) -> (Integer) rowA.getObject(0) - (Integer) rowB.getObject(0),
                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultRowMerger.DEFAULT,
                true,
                true,
                3);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
                12, 13, 14,
                15, 16, 17,
                18, 19, 20,
                21, 22, 23
        };

        int[] expectedInnerPositions = new int[] {
                0, 1, 0, 1, 2, 3,
                4, 5, 6, 7, 6, 7,
                8, 9, 8, 9, 10, 11,
                12, 13
        };

        while (it.hasNext())
        {
            Row row = it.next();
            assertFalse(row.match);
//            System.out.println(row + " " + row.getChildRows(0));
            assertEquals(expectedOuterPositions[count], row.getPos());
            if ((int) row.getObject(0) >= 5 && (int) row.getObject(0) <= 9)
            {
                assertEquals(expectedInnerPositions[(count - 12) * 2], row.getChildRows(0).get(0).getPos());
                assertEquals(expectedInnerPositions[((count - 12) * 2) + 1], row.getChildRows(0).get(1).getPos());
            }
            else
            {
                assertEquals(0, row.getChildRows(0).size());
            }
            count++;
        }

        assertEquals(24, count);
    }

    //
    //    @Test
    //    public void test_outer_join()
    //    {
    //        TableAlias a = TableAlias.of(null, "table", "a");
    //        TableAlias b = TableAlias.of(a, "tableB", "b");
    //        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
    //        Operator right = context ->
    //        {
    //            List<Row> rows = context.getOuterRows();
    //            return rows.stream().filter(row -> (Integer) row.getObject(0) >= 5).map(row -> Row.of(b, (int) row.getObject(0), new Object[] {row.getObject(0), "Val" + row.getObject(0)})).iterator();
    //        };
    //
    //        BatchMergeJoin op = new BatchMergeJoin(
    //                "",
    //                left,
    //                right,
    //                (ctx, row) -> (Integer) row.getObject(0),
    //                (ctx, row) -> (Integer) row.getObject(0),
    //                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
    //                DefaultRowMerger.DEFAULT,
    //                false,
    //                true,
    //                -1);
    //
    //        Iterator<Row> it = op.open(new OperatorContext());
    //        int count = 0;
    //        while (it.hasNext())
    //        {
    //            Row row = it.next();
    //            assertNull(row.getPredicateParent());
    //            if (count < 4)
    //            {
    //                assertEquals(emptyList(), row.getChildRows(0));
    //            }
    //            else
    //            {
    //                assertNull(row.getChildRows(0).get(0).getPredicateParent());
    //                assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
    //            }
    //            count++;
    //        }
    //
    //        assertEquals(9, count);
    //    }

    //
    //    @Test
    //    public void test_outer_join_populating()
    //    {
    //        TableAlias a = TableAlias.of(null, "table", "a");
    //        TableAlias b = TableAlias.of(a, "tableB", "b");
    //        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
    //        Operator right = context -> context.getOuterRows()
    //                .stream()
    //                .filter(row -> (Integer) row.getObject(0) >= 5)
    //                .flatMap(row -> asList(new Object[] {row.getObject(0), 1}, new Object[] {row.getObject(0), 2}).stream())
    //                .map(ar -> Row.of(b, (int) ar[0], ar))
    //                .iterator();
    //
    //        BatchMergeJoin op = new BatchMergeJoin(
    //                "",
    //                left,
    //                right,
    //                (ctx, row) -> (Integer) row.getObject(0),
    //                (ctx, row) -> (Integer) row.getObject(0),
    //                (ctx, row) -> (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
    //                DefaultRowMerger.DEFAULT,
    //                true,
    //                true,
    //                -1);
    //
    //        Iterator<Row> it = op.open(new OperatorContext());
    //        int count = 0;
    //        while (it.hasNext())
    //        {
    //            Row row = it.next();
    //            assertNull(row.getPredicateParent());
    //            if (count < 4)
    //            {
    //                assertEquals(emptyList(), row.getChildRows(0));
    //            }
    //            else
    //            {
    //                assertNull(row.getChildRows(0).get(0).getPredicateParent());
    //                assertNull(row.getChildRows(0).get(1).getPredicateParent());
    //                assertEquals(1, row.getChildRows(0).get(0).getObject(1));
    //                assertEquals(2, row.getChildRows(0).get(1).getObject(1));
    //            }
    //            count++;
    //        }
    //
    //        assertEquals(9, count);
    //    }
    //
    //    @Test
    //    public void test_nested_joins_row_hierarchy_is_correct()
    //    {
    //        TableAlias a = TableAlias.of(null, "tableA", "a");
    //        TableAlias b = TableAlias.of(a, "tableB", "b");
    //        TableAlias c = TableAlias.of(b, "tableC", "c");
    //
    //        Operator tableA = context -> IntStream.range(0, 2).mapToObj(i -> Row.of(a, i, new Object[] {i})).iterator();
    //        Operator tableB = context ->
    //        {
    //            List<Row> outerRows = context.getOuterRows();
    //            assertEquals(2, outerRows.size());
    //            assertEquals(0, outerRows.get(0).getObject(0));
    //            assertEquals(1, outerRows.get(1).getObject(0));
    //            return IntStream.range(0, 2).mapToObj(i -> Row.of(b, i, new Object[] {i, i + 10})).iterator();
    //        };
    //        Operator tableC = context ->
    //        {
    //            List<Row> outerRows = context.getOuterRows();
    //            assertEquals(2, outerRows.size());
    //            assertEquals(10, outerRows.get(0).getObject(1));
    //            assertEquals(11, outerRows.get(1).getObject(1));
    //            return IntStream.range(0, 2).mapToObj(i -> Row.of(c, i, new Object[] {i + 10})).iterator();
    //        };
    //
    //        BatchMergeJoin op = new BatchMergeJoin(
    //                "",
    //                tableA,
    //                new BatchMergeJoin(
    //                        "",
    //                        tableB,
    //                        tableC,
    //                        (ctx, row) -> (Integer) row.getObject(1),
    //                        (ctx, row) -> (Integer) row.getObject(0),
    //                        (ctx, row) ->
    //                        {
    //                            assertSame(row.getTableAlias(), c);
    //                            assertSame(row.getParent().getTableAlias(), b);
    //                            assertSame(row.getParent().getParent().getTableAlias(), a);
    //
    //                            return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(1);
    //                        },
    //                        DefaultRowMerger.DEFAULT,
    //                        false,
    //                        false,
    //                        250),
    //                (ctx, row) -> (Integer) row.getObject(0),
    //                (ctx, row) -> (Integer) row.getObject(0),
    //                (ctx, row) ->
    //                {
    //                    assertSame(row.getTableAlias(), b);
    //                    assertSame(row.getParent().getTableAlias(), a);
    //
    //                    return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
    //                },
    //                DefaultRowMerger.DEFAULT,
    //                false,
    //                false,
    //                250);
    //
    //        Iterator<Row> it = op.open(new OperatorContext());
    //        int count = 0;
    //        while (it.hasNext())
    //        {
    //            it.next();
    //            count++;
    //        }
    //
    //        assertEquals(2, count);
    //    }
    //
    //    @Ignore
    //    @Test
    //    public void test_inner_join_large()
    //    {
    //        /*
    //         *
    //         *
    //         *
    //         */
    //
    //        Random rnd = new Random();
    //        TableAlias a = TableAlias.of(null, "tableA", "a");
    //        TableAlias b = TableAlias.of(a, "tableB", "b");
    //        TableAlias c = TableAlias.of(b, "tableC", "c");
    //        Operator tableA = context -> IntStream.range(1, 1000000).mapToObj(i -> Row.of(a, i, new Object[] {i, rnd.nextBoolean()})).iterator();
    //        Operator tableB = context ->
    //        {
    //            return context.getOuterRows()
    //                    .stream()
    //                    .flatMap(row -> asList(
    //                            new Object[] {row.getObject(0), 1, rnd.nextBoolean()},
    //                            new Object[] {row.getObject(0), 2, rnd.nextBoolean()},
    //                            new Object[] {row.getObject(0), 3, rnd.nextBoolean()},
    //                            new Object[] {row.getObject(0), 4, rnd.nextBoolean()}).stream())
    //                    .map(ar2 -> Row.of(b, (int) ar2[0], ar2))
    //                    .iterator();
    //        };
    //        Operator tableC = context ->
    //        {
    //            return context.getOuterRows()
    //                    .stream()
    //                    .flatMap(row -> asList(
    //                            new Object[] {row.getObject(0), 1, "Val" + row.getObject(0)},
    //                            new Object[] {row.getObject(0), 4, rnd.nextBoolean()}).stream())
    //                    .map(ar -> Row.of(c, (int) ar[0], ar))
    //                    .iterator();
    //        };
    //
    //        /**
    //         * from tableA a inner join [ tableB b inner join [tableC] c on c.id = b.id ] b on b.id = a.id
    //         */
    //
    //        BatchMergeJoin op = new BatchMergeJoin(
    //                "",
    //                tableA,
    //                new BatchMergeJoin(
    //                        "",
    //                        tableB,
    //                        tableC,
    //                        (ctx, row) -> (int) row.getObject(0),
    //                        (ctx, row) -> (int) row.getObject(0),
    //                        (ctx, row) ->
    //                        {
    //                            return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
    //                        },
    //                        DefaultRowMerger.DEFAULT,
    //                        false,
    //                        false,
    //                        250),
    //                (ctx, row) -> (int) row.getObject(0),
    //                (ctx, row) -> (int) row.getObject(0),
    //                (ctx, row) ->
    //                {
    //                    return (Boolean) row.getObject(2)
    //                        && (Boolean) row.getParent().getObject(1)
    //                        && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
    //                },
    //                DefaultRowMerger.DEFAULT,
    //                true,
    //                false,
    //                250);
    //
    //        for (int i = 0; i < 150; i++)
    //        {
    //            StopWatch sw = new StopWatch();
    //            sw.start();
    //            Iterator<Row> it = op.open(new OperatorContext());
    //            int count = 0;
    //            while (it.hasNext())
    //            {
    //                Row row = it.next();
    //                //                assertEquals(4, row.getChildRows(0).size());
    //                //                            System.out.println(row);
    //                //            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
    //                count++;
    //            }
    //            sw.stop();
    //            long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    //            System.out.println("Time: " + sw.toString() + " rows: " + count + " mem: " + FileUtils.byteCountToDisplaySize(mem));
    //        }
    //        //        assertEquals(5, count);
    //    }
}
