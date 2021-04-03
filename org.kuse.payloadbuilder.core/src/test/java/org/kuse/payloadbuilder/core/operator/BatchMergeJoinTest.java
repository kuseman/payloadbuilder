package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Ignore;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test {@link BatchMergeJoin} */
@Ignore("Need to fix all expression to be resolved correctly when manually creating operator")
public class BatchMergeJoinTest extends AOperatorTest
{
    private final Index index = new Index(QualifiedName.of("table"), asList("col"), 10);
    private final TableAlias a = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
            .columns(new String[] {"col1", "col2"})
            .children(asList(
                    TableAliasBuilder.of(1, TableAlias.Type.TABLE, QualifiedName.of("tableB"), "b")
                            .columns(new String[] {"col1", "col2"})))
            .build();
    private final TableAlias b = a.getChildAliases().get(0);

    @Test
    public void test_inner_join_empty_outer()
    {
        Operator left = op(context -> emptyIterator());
        Operator right = op(context -> emptyIterator());

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                -1);

        assertFalse(op.open(new ExecutionContext(session)).hasNext());
    }

    @Test
    public void test_inner_join_empty_inner()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            while (context.getOperatorContext().getOuterIndexValues().hasNext())
            {
                context.getOperatorContext().getOuterIndexValues().next();
            }
            return emptyIterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                -1);

        assertFalse(op.open(new ExecutionContext(session)).hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_bad_implementation_of_inner_operator()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context -> emptyIterator());

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                2);

        op.open(new ExecutionContext(session)).hasNext();
    }

    @Test(expected = NoSuchElementException.class)
    public void test_bad_implementation_of_inner_operator_3()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            while (context.getOperatorContext().getOuterIndexValues().hasNext())
            {
                context.getOperatorContext().getOuterIndexValues().next();
            }
            context.getOperatorContext().getOuterIndexValues().next();
            return emptyIterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                2);

        op.open(new ExecutionContext(session)).hasNext();
    }

    @Test
    public void test_inner_join_one_to_one()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                3);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] expectedOuterPositions = new int[] {
                7,
                8, 9, 10,
                11
        };

        int[] expectedInnerPositions = new int[] {
                0,
                1, 2, 3,
                4
        };

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_one()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                true,
                index,
                3);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] expectedOuterPositions = new int[] {
                0, 1, 2,
                3, 4, 5,
                6, 7, 8,
                9, 10, 11,
        };

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null,
                null, null, 0,
                1, 2, 3,
                4, null, null
        };

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(12, count);
    }

    @Test
    public void test_inner_join_one_to_many()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                2);

        RowIterator it = op.open(new ExecutionContext(session));
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
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_one_to_many()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                true,
                index,
                3);

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
                null, null, null,
                null, null, null,
                null, 0, 1, 2, 3,         // Batch 1
                4, 5, 6, 7, 8, 9,        // Batch 2
                null, null
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(19, count);
    }

    @Test
    public void test_inner_join_many_to_one()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableInt batchCount = new MutableInt();

        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            batchCount.increment();
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                5);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 14, 15, 16, 17,
                18, 19, 20, 21
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1, 1, 2, 2,
                3, 3, 4, 4
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(4, batchCount.intValue());
        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_many_to_one()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableInt batchCount = new MutableInt();

        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            batchCount.increment();
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                false,
                true,
                index,
                3);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2, 3,
                4, 5, 6, 7,
                8, 9, 10, 11,
                12, 13, 14, 15,
                16, 17, 18, 19,
                20, 21, 22, 23
        };

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null, null,
                null, null, null, null,
                null, null, null, null,
                0, 0, 1, 1,
                2, 2, 3, 3,
                4, 4, null, null
        };

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(6, batchCount.intValue());
        assertEquals(24, count);
    }

    @Test
    public void test_inner_join_many_to_many()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableInt batchCount = new MutableInt();

        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            batchCount.increment();
            // Create 2 rows for each input row
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                //                (ctx, row, values) -> values[0] = row.getObject(0),
                //                (ctx, row, values) -> values[0] = row.getObject(0),
                //                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                new DefaultTupleMerger(-1, 1),
                false,
                false,
                index,
                3);

        int[] expectedOuterPositions = new int[] {
                12, 13, 12, 13, 14, 15, 14, 15,
                16, 17, 16, 17, 18, 19, 18, 19,
                20, 21, 20, 21
        };

        int[] expectedInnerPositions = new int[] {
                0, 0, 1, 1, 2, 2, 3, 3,
                4, 4, 5, 5, 6, 6, 7, 7,
                8, 8, 9, 9
        };

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(6, batchCount.intValue());
        assertEquals(20, count);
    }

    @Test
    public void test_outer_join_many_to_many()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableInt batchCount = new MutableInt();

        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            batchCount.increment();
            // Create 2 rows for each input row
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}, new Object[] {val, 3}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1 and b.col2 < 3")),
                new DefaultTupleMerger(-1, 1),
                false,
                true,
                index,
                3);

        int[] expectedOuterPositions = new int[] {
                0, 1, 2, 3,
                4, 5, 6, 7,
                8, 9, 10, 11,
                12, 13, 12, 13, 14, 15, 14, 15,
                16, 17, 16, 17, 18, 19, 18, 19,
                20, 21, 20, 21, 22, 23
        };

        Integer[] expectedInnerPositions = new Integer[] {
                null, null, null, null,
                null, null, null, null,
                null, null, null, null,
                0, 0, 1, 1, 3, 3, 4, 4,
                6, 6, 7, 7, 9, 9, 10, 10,
                12, 12, 13, 13, null, null
        };

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));
            assertEquals(expectedInnerPositions[count], tuple.getTuple(1).getValue("__pos"));
            count++;
        }

        assertEquals(6, batchCount.intValue());
        assertEquals(34, count);
    }

    @Test
    public void test_inner_join_one_to_one_populating()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                true,
                false,
                index,
                3);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                7, 8,       // Batch 1
                9, 10, 11   // Batch 2
        };

        List<List<Integer>> expectedInnerPositions = asList(
                asList(0), asList(1),               // Batch 1
                asList(2), asList(3), asList(4)     // Batch 2
        );

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertEquals("" + count, expectedInnerPositions.get(count), col.stream().map(t -> (int) t.getValue("__pos")).collect(toList()));
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_one_populating()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                true,
                true,
                index,
                3);

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

        assertEquals(14, count);
    }

    @Test
    public void test_inner_join_one_to_many_populating()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                true,
                false,
                index,
                2);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] expectedOuterPositions = new int[] {
                7,
                8, 9,
                10, 11
        };

        List<List<Integer>> expectedInnerPositions = asList(
                asList(0, 1),       // Batch 1
                asList(2, 3), asList(4, 5), // Batch 2
                asList(6, 7), asList(8, 9)  // Batch 3
        );

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

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_many_populating()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> IntStream.range(-2, 12).mapToObj(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                true,
                true,
                index,
                2);

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
                null, asList(0, 1),
                asList(2, 3), asList(4, 5),
                asList(6, 7), asList(8, 9),
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

        assertEquals(14, count);
    }

    @Test
    public void test_inner_join_many_to_many_populating()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableInt batchCount = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            batchCount.increment();
            // Create 2 rows for each input row
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1),
                true,
                false,
                index,
                3);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                12, 13, 14, 15,
                16, 17, 18, 19,
                20, 21
        };

        List<List<Integer>> expectedInnerPositions = asList(
                asList(0, 1), asList(0, 1), asList(2, 3), asList(2, 3),
                asList(4, 5), asList(4, 5), asList(6, 7), asList(6, 7),
                asList(8, 9), asList(8, 9), asList(10, 11));

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue("__pos")).collect(toList()) : null);
            count++;
        }
        assertEquals(6, batchCount.intValue());
        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_many_to_many_populating()
    {
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableInt batchCount = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            batchCount.increment();
            // Create 2 rows for each input row
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            return StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchMergeJoin op = new BatchMergeJoin(
                0, "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                //                    (ctx, row, values) -> values[0] = row.getObject(0),
                //                    (ctx, row, values) -> values[0] = row.getObject(0),
                //                    (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                new DefaultTupleMerger(-1, 1),
                true,
                true,
                index,
                3);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;

        int[] expectedOuterPositions = new int[] {
                0, 1, 2, 3,
                4, 5, 6, 7,
                8, 9, 10, 11,
                12, 13, 14, 15,
                16, 17, 18, 19,
                20, 21, 22, 23
        };

        List<List<Integer>> expectedInnerPositions = asList(
                null, null, null, null,
                null, null, null, null,
                null, null, null, null,
                asList(0, 1), asList(0, 1), asList(2, 3), asList(2, 3),
                asList(4, 5), asList(4, 5), asList(6, 7), asList(6, 7),
                asList(8, 9), asList(8, 9), null, null,
                null, null, null, null);

        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(expectedOuterPositions[count], tuple.getTuple(0).getValue("__pos"));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getTuple(1);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue("__pos")).collect(toList()) : null);
            count++;
        }

        assertEquals(6, batchCount.intValue());
        assertEquals(24, count);
    }

    //    @Ignore
    //    @Test
    //    public void test_inner_join_large()
    //    {
    //        Random rnd = new Random();
    //        TableAlias a = TableAlias.of(null, "tableA", "a");
    //        TableAlias b = TableAlias.of(a, "tableB", "b");
    //        TableAlias c = TableAlias.of(b, "tableC", "c");
    //        MutableInt bPos = new MutableInt();
    //        MutableInt cPos = new MutableInt();
    //
    //        Operator tableA = op(context -> IntStream.range(1, 400000).mapToObj(i -> Row.of(a, i, new Object[] {i, rnd.nextBoolean()})).iterator());
    //        Operator tableB = op(context ->
    //        {
    //            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
    //            return StreamSupport.stream(it.spliterator(), false)
    //                    .map(ar -> (Integer) ar[0])
    //                    .flatMap(val -> asList(
    //                            new Object[] {val, 1, rnd.nextBoolean()},
    //                            new Object[] {val, 2, rnd.nextBoolean()},
    //                            new Object[] {val, 3, rnd.nextBoolean()},
    //                            new Object[] {val, 4, rnd.nextBoolean()}).stream())
    //                    .map(ar2 -> Row.of(b, bPos.getAndIncrement(), ar2))
    //                    .iterator();
    //        });
    //        Operator tableC = op(context ->
    //        {
    //            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
    //
    //            return StreamSupport.stream(it.spliterator(), false)
    //                    .map(ar -> (Integer) ar[0])
    //                    .flatMap(val -> asList(
    //                            new Object[] {val, 1, "Val" + val},
    //                            new Object[] {val, 4, rnd.nextBoolean()}).stream())
    //                    .map(ar -> Row.of(c, cPos.getAndIncrement(), ar))
    //                    .iterator();
    //        });
    //
    //        /**
    //         * from tableA a inner join [ tableB b inner join [tableC] c on c.id = b.id ] b on b.id = a.id
    //         */
    //
    //        BatchMergeJoin op = new BatchMergeJoin(
    //                0, "",
    //                tableA,
    //                new BatchMergeJoin(
    //                        0, "",
    //                        tableB,
    //                        tableC,
    //                        (ctx, row, values) -> values[0] = row.getObject(0),
    //                        (ctx, row, values) -> values[0] = row.getObject(0),
    //                        (ctx, row) ->
    //                        {
    //                            return (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
    //                        },
    //                        DefaultRowMerger.DEFAULT,
    //                        false,
    //                        false,
    //                        index,
    //                        250),
    //                (ctx, row, values) -> values[0] = row.getObject(0),
    //                (ctx, row, values) -> values[0] = row.getObject(0),
    //                (ctx, row) ->
    //                {
    //                    return (Boolean) row.getObject(2)
    //                        && (Boolean) row.getParent().getObject(1)
    //                        && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0);
    //                },
    //                DefaultRowMerger.DEFAULT,
    //                false,
    //                false,
    //                index,
    //                250);
    //
    //        for (int i = 0; i < 150; i++)
    //        {
    //            StopWatch sw = new StopWatch();
    //            sw.start();
    //            Iterator<Row> it = op.open(new ExecutionContext(session));
    //            int count = 0;
    //            while (it.hasNext())
    //            {
    //                Row row = it.next();
    //                //                assertEquals(4, row.getChildRows(0).size());
    //                //                System.out.println(row + " " + row.getChildRows(0).stream().map(r -> r.toString() + " " + r.getChildRows(0)).collect(joining(", ")) );
    //                //            assertEquals("Val" + row.getObject(0), row.getChildRows(0).get(0).getObject(1));
    //                count++;
    //            }
    //            sw.stop();
    //            long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    //            System.out.println("Time: " + sw.toString() + " rows: " + count + " mem: " + FileUtils.byteCountToDisplaySize(mem));
    //        }
    //    }
}
