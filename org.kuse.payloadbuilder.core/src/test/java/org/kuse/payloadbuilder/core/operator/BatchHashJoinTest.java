package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test {@link BatchHashJoin} */
public class BatchHashJoinTest extends AOperatorTest
{
    @Test
    public void test_inner_join_empty_outer()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 1);
        Operator left = op(ctx -> RowIterator.EMPTY);
        Operator right = op(ctx -> RowIterator.EMPTY);

        BatchHashJoin op = new BatchHashJoin(
                0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 0, 2),
                false,
                false,
                index,
                null);

        assertFalse(op.open(new ExecutionContext(session)).hasNext());
    }

    @Test
    public void test_inner_join_empty_inner()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();

        Index index = new Index(QualifiedName.of("table"), asList("col1"), 1);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(ctx ->
                {
                    while (ctx.getOperatorContext().getOuterIndexValues().hasNext())
                    {
                        ctx.getOperatorContext().getOuterIndexValues().next();
                    }
                    return RowIterator.EMPTY;
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
        it.close();
        assertEquals(1, aClose.intValue());
        // Batch size 1
        assertEquals(9, bClose.intValue());
    }

    @Test
    public void test_bad_implementation_of_inner_operator()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();

        Index index = new Index(QualifiedName.of("table"), asList("col1"), 1);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(ctx -> RowIterator.EMPTY, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        try
        {
            op.open(new ExecutionContext(session)).hasNext();
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("Check implementation of operator with index: table [col1] not all outer values was processed."));
        }
    }

    @Test
    public void test_bad_implementation_of_inner_operator_2()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();

        Index index = new Index(QualifiedName.of("table"), asList("col1"), 2);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(ctx ->
                {
                    ctx.getOperatorContext().getOuterIndexValues().hasNext();
                    Object[] ar = ctx.getOperatorContext().getOuterIndexValues().next();
                    return asList((Tuple) Row.of(a, 0, ar)).iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        try
        {
            op.open(new ExecutionContext(session)).hasNext();
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("Check implementation of operator with index: table [col1] not all outer values was processed."));
        }
    }

    @Test
    public void test_bad_implementation_of_inner_operator_3()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();

        Index index = new Index(QualifiedName.of("table"), asList("col1"), 2);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(ctx ->
                {
                    while (ctx.getOperatorContext().getOuterIndexValues().hasNext())
                    {
                        ctx.getOperatorContext().getOuterIndexValues().next();
                    }
                    ctx.getOperatorContext().getOuterIndexValues().next();
                    return RowIterator.EMPTY;
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        try
        {
            op.open(new ExecutionContext(session)).hasNext();
            fail();
        }
        catch (NoSuchElementException e)
        {
        }
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
            "  select ** " +
            "  from tableB b " +
            "  inner join tableC c " +
            "     on col2 = a.col2 " +
            "     and c.col1 = b.col1 " +
            ") b " +
            "  on b.col1 = a.col1";

        MutableInt aCloseCount = new MutableInt();
        MutableInt bCloseCount = new MutableInt();
        MutableInt cCloseCount = new MutableInt();

        Index index = new Index(QualifiedName.of("tableC"), asList("col1"), 3);
        AtomicInteger posC = new AtomicInteger();
        Operator op = operator(query,
                ofEntries(entry("tableC", index)),
                ofEntries(entry("tableC", a -> op(ctx ->
                {
                    Iterable<Object[]> it = () -> ctx.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .map(val -> (Tuple) Row.of(a, posC.getAndIncrement(), new String[] {"col1", "col2"}, new Object[] {val, "val" + val}))
                            .collect(toList());

                    return inner.iterator();
                }, () -> cCloseCount.increment()))),
                ofEntries(
                        entry("tableA",
                                a -> op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> (Tuple) Row.of(a, i, new String[] {"col1", "col2"}, new Object[] {i, "val" + i})).iterator(),
                                        () -> aCloseCount.increment())),
                        entry("tableB",
                                a -> op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(a, 10 * i, new String[] {"col1"}, new Object[] {i})).iterator(), () -> bCloseCount.increment()))

                ));

        assertTrue("A Nested loop should have been constructed", op instanceof NestedLoopJoin);
        assertTrue("A Batch Hash join should have been constructed as inner operator", ((NestedLoopJoin) op).getInner() instanceof BatchHashJoin);

        int[] tableAPos = new int[] {4, 5};
        int[] tableBPos = new int[] {40, 50};
        int[] tableCPos = new int[] {12, 17};

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

        assertEquals(2, count);
        assertEquals(1, aCloseCount.intValue());
        // Open/closed the same amount as outer operator rows (nested loop)
        assertEquals(5, bCloseCount.intValue());
        // Batch size 3, tableB has 4 rows, 2 batches per operator call, opened 5 times => 2 * 5 = 10
        assertEquals(10, cCloseCount.intValue());
    }

    @Test
    public void test_inner_join_one_to_one()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .map(val -> Row.of(a, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                            .collect(toList());

                    return inner.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));

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

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }
        it.close();

        assertEquals(5, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_outer_join_one_to_one()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .map(val -> Row.of(a, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                            .collect(toList());

                    return inner.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,    // Batch 1
                3, 4, 5,    // Batch 2
                6, 7, 8,    // Batch 3
                9, 10, 11,  // Batch 4
        };

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null,   // Batch 1
                null, null, 0,      // Batch 2
                1, 2, 3,            // Batch 3
                4, null, null       // Batch 4
        };

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            Integer val = Optional.ofNullable(tuple.getTuple(1)).map(t -> (Integer) t.getValue("__pos")).orElse(null);
            assertEquals(expectedInnerPositions[count], val);
            count++;
        }
        it.close();
        assertEquals(12, count);
        assertEquals(1, aClose.intValue());
        assertEquals(4, bClose.intValue());
    }

    @Test
    public void test_inner_join_one_to_many()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    return StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}, new Object[] {-666, 3}).stream())
                            .map(ar -> (Tuple) Row.of(a, posRight.getAndIncrement(), ar))
                            .iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                7, 7,
                8, 8,
                9, 9,
                10, 10,
                11, 11
        };

        int[] expectedInnerPositions = new int[] {
                0, 1, 3, 4,
                6, 7, 9, 10,
                12, 13
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }
        it.close();

        assertEquals(10, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_outer_join_one_to_many()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    return StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                            .map(ar -> (Tuple) Row.of(a, posRight.getAndIncrement(), ar))
                            .iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,                // Batch 1
                3, 4, 5,                // Batch 2
                6, 7, 7, 8, 8,          // Batch 3
                9, 9, 10, 10, 11, 11,   // Batch 4
                12, 13                  // Batch 5
        };

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null,   // Batch 1
                null, null, null,   // Batch 2
                null, 0, 1, 2, 3,   // Batch 3
                4, 5, 6, 7, 8, 9,   // Batch 4
                null, null          // Batch 5
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            Integer val = Optional.ofNullable(tuple.getTuple(1)).map(t -> (Integer) t.getValue("__pos")).orElse(null);
            assertEquals(expectedInnerPositions[count], val);
            count++;
        }
        it.close();

        assertEquals(19, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_inner_join_many_to_one()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 5);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> rows = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .map(val -> (Tuple) Row.of(a, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                            .collect(toList());

                    return rows.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 14,         // Batch 1
                15, 16, 17, 18, 19, // Batch 2
                20, 21              // Batch 3
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1,        // Batch 1
                1, 2, 2, 3, 3,  // Batch 2
                4, 4            // Batch 2
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }
        it.close();

        assertEquals(10, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_outer_join_many_to_one()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> rows = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .map(val -> (Tuple) Row.of(a, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                            .collect(toList());

                    return rows.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
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

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null,
                null, null, null,
                null, null, null,
                null, null, null,
                0, 0, 1,        // Batch 1
                1, 2, 2,        // Batch 2
                3, 3, 4,        // Batch 2
                4, null, null
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            Integer val = Optional.ofNullable(tuple.getTuple(1)).map(t -> (Integer) t.getValue("__pos")).orElse(null);
            assertEquals(expectedInnerPositions[count], val);
            count++;
        }
        it.close();

        assertEquals(24, count);
        assertEquals(1, aClose.intValue());
        assertEquals(8, bClose.intValue());
    }

    @Test
    public void test_inner_join_many_to_many()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b "
            + "  on b.col1 = a.col1";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    // Create 2 rows for each input row
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                            .map(ar -> Row.of(a, posRight.getAndIncrement(), ar))
                            .collect(toList());

                    return inner.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 12, 13, 13, 14, 14,
                15, 15, 16, 16, 17, 17,
                18, 18, 19, 19, 20, 20,
                21, 21
        };

        int[] expectedInnerPositions = new int[] {
                0, 1, 0, 1, 2, 3,
                2, 3, 4, 5, 4, 5,
                6, 7, 6, 7, 8, 9,
                8, 9
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }
        it.close();

        assertEquals(20, count);
        assertEquals(1, aClose.intValue());
        assertEquals(8, bClose.intValue());
    }

    @Test
    public void test_outer_join_many_to_many()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b "
            + "  on b.col1 = a.col1 "
            + "  and b.col2 < 3 ";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    // Create 3 rows for each input row
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}, new Object[] {val, 3}).stream())
                            .map(ar -> Row.of(a, posRight.getAndIncrement(), new String[] {"col1", "col2"}, ar))
                            .collect(toList());

                    return inner.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
                12, 12, 13, 13, 14, 14,
                15, 15, 16, 16, 17, 17,
                18, 18, 19, 19, 20, 20,
                21, 21, 22, 23
        };

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null,
                null, null, null,
                null, null, null,
                null, null, null,
                0, 1, 0, 1, 3, 4,
                3, 4, 6, 7, 6, 7,
                9, 10, 9, 10, 12, 13,
                12, 13, null, null
        };

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals("Count :" + count, expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            Integer val = Optional.ofNullable(tuple.getTuple(1)).map(t -> (Integer) t.getValue("__pos")).orElse(null);
            assertEquals("Count: " + count, expectedInnerPositions[count], val);
            count++;
        }
        it.close();

        assertEquals(34, count);
        assertEquals(1, aClose.intValue());
        assertEquals(8, bClose.intValue());
    }

    @Test
    public void test_inner_join_one_to_one_populating()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b with(populate=True) "
            + "  on b.col1 = a.col1 ";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .map(val -> Row.of(a, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                            .collect(toList());

                    return inner.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
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
            Tuple tuple = it.next();
            assertEquals("Count: " + count, expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);
            assertArrayEquals("Count: " + count, new int[] {expectedInnerPositions[count]}, col.stream().mapToInt(t -> (int) t.getValue("__pos")).toArray());
            count++;
        }
        it.close();

        assertEquals(5, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_outer_join_one_to_one_populating()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b with(populate=True) "
            + "  on b.col1 = a.col1 ";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .map(val -> Row.of(a, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                            .collect(toList());

                    return inner.iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
                12, 13
        };

        List<List<Integer>> expectedInnerPositions = asList(
                null, null, null,
                null, null, null,
                null, asList(0), asList(1),
                asList(2), asList(3), asList(4),
                null, null);

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue("__pos")).collect(toList()) : null);
            count++;
        }
        it.close();

        assertEquals(14, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_inner_join_one_to_many_populating()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b with(populate=True) "
            + "  on b.col1 = a.col1 ";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    return StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                            .map(ar -> (Tuple) Row.of(a, posRight.getAndIncrement(), ar))
                            .iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] expectedOuterPositions = new int[] {
                7,
                8, 9,
                10, 11
        };

        int[][] expectedInnerPositions = new int[][] {
                new int[] {0, 1},
                new int[] {2, 3},
                new int[] {4, 5},
                new int[] {6, 7},
                new int[] {8, 9}
        };

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertArrayEquals("" + count, expectedInnerPositions[count], col.stream().mapToInt(t -> (int) t.getValue("__pos")).toArray());
            count++;
        }
        it.close();

        assertEquals(5, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_outer_join_one_to_many_populating()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b with(populate=True) "
            + "  on b.col1 = a.col1 ";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    return StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                            .map(ar -> (Tuple) Row.of(a, posRight.getAndIncrement(), ar))
                            .iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] expectedOuterPositions = new int[] {
                0, 1,
                2, 3,
                4, 5,
                6, 7,
                8, 9,
                10, 11,
                12, 13
        };

        List<List<Integer>> expectedInnerPositions = asList(
                null, null,
                null, null,
                null, null,
                null,

                asList(0, 1),
                asList(2, 3),
                asList(4, 5),
                asList(6, 7),
                asList(8, 9),

                null, null);

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue("__pos")).collect(toList()) : null);
            count++;
        }
        it.close();

        assertEquals(14, count);
        assertEquals(1, aClose.intValue());
        assertEquals(5, bClose.intValue());
    }

    @Test
    public void test_inner_join_many_to_many_populating()
    {
        String query = "select * "
            + "from tableA a "
            + "inner join tableB b with(populate=True) "
            + "  on b.col1 = a.col1 "
            + "  and a.col1 > 0";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    // Create 2 rows for each input row
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    return StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                            .map(ar -> (Tuple) Row.of(a, posRight.getAndIncrement(), ar))
                            .iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 14,
                15, 16, 17,
                18, 19, 20,
                21,
        };

        int[][] expectedInnerPositions = new int[][] {
                new int[] {0, 1}, new int[] {0, 1}, new int[] {2, 3},
                new int[] {2, 3}, new int[] {4, 5}, new int[] {4, 5},
                new int[] {6, 7}, new int[] {6, 7}, new int[] {8, 9},
                new int[] {8, 9}
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals("Count: " + count, expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertArrayEquals("Count: " + count, expectedInnerPositions[count], col.stream().mapToInt(t -> (int) t.getValue("__pos")).toArray());
            count++;
        }
        it.close();

        assertEquals(10, count);
        assertEquals(1, aClose.intValue());
        assertEquals(8, bClose.intValue());
    }

    @Test
    public void test_outer_join_many_to_many_populating()
    {
        String query = "select * "
            + "from tableA a "
            + "left join tableB b with(populate=True) "
            + "  on b.col1 = a.col1 "
            + "  and a.col1 > 0";

        MutableInt aClose = new MutableInt();
        MutableInt bClose = new MutableInt();
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();

        Index index = new Index(QualifiedName.of("tableB"), asList("col1"), 3);
        Operator op = operator(query,
                ofEntries(entry("tableB", index)),
                ofEntries(entry("tableB", a -> op(context ->
                {
                    // Create 2 rows for each input row
                    Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
                    return StreamSupport.stream(it.spliterator(), false)
                            .map(ar -> (Integer) ar[0])
                            .filter(val -> val >= 5 && val <= 9)
                            .distinct()
                            .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                            .map(ar -> (Tuple) Row.of(a, posRight.getAndIncrement(), ar))
                            .iterator();
                }, () -> bClose.increment()))),
                ofEntries(entry("tableA", a -> op(ctx -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                        .stream()
                        .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                        .iterator(), () -> aClose.increment()))));

        assertTrue("BatchHashJoin should have been constructed", op instanceof BatchHashJoin);

        RowIterator it = op.open(new ExecutionContext(session));
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

        List<List<Integer>> expectedInnerPositions = asList(
                null, null, null,
                null, null, null,
                null, null, null,
                null, null, null,
                asList(0, 1), asList(0, 1), asList(2, 3),
                asList(2, 3), asList(4, 5), asList(4, 5),
                asList(6, 7), asList(6, 7), asList(8, 9),
                asList(8, 9), null, null);

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue("__pos")).collect(toList()) : null);
            count++;
        }
        it.close();

        assertEquals(24, count);
        assertEquals(1, aClose.intValue());
        assertEquals(8, bClose.intValue());
    }

    //        @Ignore
    //    @Test
    //    public void test_inner_join_large()
    //    {
    //        Random rnd = new Random();
    //        //            TableAlias a = TableAlias.of(null, "tableA", "a");
    //        //            TableAlias b = TableAlias.of(a, "tableB", "b");
    //        //            TableAlias c = TableAlias.of(b, "tableC", "c");
    //
    //        MutableInt bPos = new MutableInt();
    //        MutableInt cPos = new MutableInt();
    //        Index indexB = new Index(QualifiedName.of("tableB"), asList("col1"), 250);
    //        Index indexC = new Index(QualifiedName.of("tableC"), asList("col1"), 250);
    //
    //        String query = "select * "
    //            + "from tableA a "
    //            + "inner join tableB b "
    //            + "  on b.col1 = a.col1 "
    //            + "  and b.col2 "
    //            + "inner join tableC c "
    //            + "  on c.col1 = b.col1 "
    //            + "  and c.col2 "
    //            + "where a.col2 ";
    //
    //        Operator op = operator(query,
    //                MapUtils.ofEntries(
    //                        MapUtils.entry("tableB", indexB),
    //                        MapUtils.entry("tableC", indexC)),
    //                MapUtils.ofEntries(
    //                        MapUtils.entry("tableB", a -> op(context ->
    //                        {
    //                            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
    //                            return StreamSupport.stream(it.spliterator(), false)
    //                                    .map(ar -> (Integer) ar[0])
    //                                    .flatMap(val -> asList(
    //                                            new Object[] {val, 1, rnd.nextBoolean()},
    //                                            new Object[] {val, 2, rnd.nextBoolean()},
    //                                            new Object[] {val, 3, rnd.nextBoolean()},
    //                                            new Object[] {val, 4, rnd.nextBoolean()}).stream())
    //                                    .map(ar2 -> (Tuple) Row.of(a, bPos.getAndIncrement(), new String[] {"col1", "col2"}, ar2))
    //                                    .iterator();
    //                        })),
    //                        MapUtils.entry("tableC", a -> op(context ->
    //                        {
    //                            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
    //                            return StreamSupport.stream(it.spliterator(), false)
    //                                    .map(ar -> (Integer) ar[0])
    //                                    .flatMap(val -> asList(
    //                                            new Object[] {val, 1, "Val" + val},
    //                                            new Object[] {val, 4, rnd.nextBoolean()}).stream())
    //                                    .map(ar -> (Tuple) Row.of(a, cPos.getAndIncrement(), ar))
    //                                    .iterator();
    //                        }))),
    //                MapUtils.ofEntries(
    //                        MapUtils.entry("tableA",
    //                                a -> op(ctx -> IntStream.range(1, 10000000)
    //                                        .mapToObj(i -> (Tuple) Row.of(a, i, new String[] {"col1", "col2"}, new Object[] {rnd.nextInt(10000), rnd.nextBoolean()}))
    //                                        .iterator()))));
    //
    //        for (int i = 0; i < 150; i++)
    //        {
    //            StopWatch sw = new StopWatch();
    //            sw.start();
    //            Iterator<Tuple> it = op.open(new ExecutionContext(session));
    //            int count = 0;
    //            while (it.hasNext())
    //            {
    //                Tuple tuple = it.next();
    //                //                assertEquals(4, row.getChildRows(0).size());
    //                //                                                                System.out.println(row + " " + row.getChildRows(0).stream().map(r -> r.toString() + " " + r.getChildRows(0)).collect(joining(", ")) );
    //                //            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
    //                count++;
    //            }
    //            sw.stop();
    //            long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    //            System.out.println("Time: " + sw.toString() + " rows: " + count + " mem: " + FileUtils.byteCountToDisplaySize(mem));
    //        }
    //        assertEquals(5, count);
    //    }
}
