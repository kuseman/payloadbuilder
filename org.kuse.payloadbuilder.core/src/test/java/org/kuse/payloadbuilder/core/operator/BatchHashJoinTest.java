package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test {@link BatchHashJoin} */
public class BatchHashJoinTest extends AOperatorTest
{
    private final TableAlias a = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
            .columns(new String[] {"col1", "col2"})
            .children(asList(
                    TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableB"), "b")
                            .columns(new String[] {"col1", "col2"})
                            .children(asList(
                                    TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableC"), "c")
                                            .columns(new String[] {"col1", "col2"})))))
            .build();
    private final TableAlias b = a.getChildAliases().get(0);
    private final TableAlias c = b.getChildAliases().get(0);

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
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index,
                null);

        assertFalse(op.open(new ExecutionContext(session)).hasNext());
    }

    @Test
    public void test_inner_join_empty_inner()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 1);
        Operator left = op(contet -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            while (context.getOperatorContext().getOuterIndexValues().hasNext())
            {
                context.getOperatorContext().getOuterIndexValues().next();
            }
            return RowIterator.EMPTY;
        });
        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index,
                null);

        assertFalse(op.open(new ExecutionContext(session)).hasNext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_bad_implementation_of_inner_operator()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 2);
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context -> RowIterator.EMPTY);

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index,
                null);

        op.open(new ExecutionContext(session)).hasNext();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_bad_implementation_of_inner_operator_2()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 2);
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            context.getOperatorContext().getOuterIndexValues().hasNext();
            Object[] ar = context.getOperatorContext().getOuterIndexValues().next();
            return asList((Tuple) Row.of(a, 0, ar)).iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index,
                null);

        op.open(new ExecutionContext(session)).hasNext();
    }

    @Test(expected = NoSuchElementException.class)
    public void test_bad_implementation_of_inner_operator_3()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 2);
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context ->
        {
            while (context.getOperatorContext().getOuterIndexValues().hasNext())
            {
                context.getOperatorContext().getOuterIndexValues().next();
            }
            context.getOperatorContext().getOuterIndexValues().next();
            return RowIterator.EMPTY;
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index,
                null);

        op.open(new ExecutionContext(session)).hasNext();
    }

    @Test
    public void test_correlated()
    {
        // Test that a correlated query with a batch hash join
        // uses the context row into consideration when joining

        Index index = new Index(QualifiedName.of("tableA"), asList("col"), 3);

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

        Operator opA = op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i, "val" + i})).iterator());
        Operator opB = op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(b, 10 * i, new Object[] {i})).iterator());

        AtomicInteger posC = new AtomicInteger();
        Operator opC = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .map(val -> (Tuple) Row.of(c, posC.getAndIncrement(), new Object[] {val, "val" + val}))
                    .collect(toList());

            return inner.iterator();
        });

        Operator op = new NestedLoopJoin(
                0,
                "",
                opA,
                new BatchHashJoin(
                        1,
                        "",
                        opB,
                        opC,
                        new ExpressionValuesExtractor(asList(e("b.col1"))),
                        new ExpressionValuesExtractor(asList(e("c.col1"))),
                        new ExpressionPredicate(e("c.col1 = b.col1 and c.col2 = a.col2")),
                        DefaultTupleMerger.DEFAULT,
                        false,
                        false,
                        index,
                        null),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false);

        int[] tableAPos = new int[] {4, 5};
        int[] tableBPos = new int[] {40, 50};
        int[] tableCPos = new int[] {12, 17};

        int count = 0;
        RowIterator it = op.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(tableBPos[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            assertEquals(tableCPos[count], tuple.getValue(QualifiedName.of("c", "__pos"), 0));
            count++;
        }

        assertEquals(2, count);
    }

    @Test
    public void test_inner_join_one_to_one()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        MutableBoolean leftClose = new MutableBoolean();
        MutableInt rightClose = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator(), () -> leftClose.setTrue());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        }, () -> rightClose.increment());

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index,
                null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }
        it.close();

        assertEquals(5, count);
        assertTrue(leftClose.booleanValue());
        assertEquals(5, rightClose.intValue());
    }

    @Test
    public void test_outer_join_one_to_one()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(12, count);
    }

    @Test
    public void test_inner_join_one_to_many()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 2);
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
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}, new Object[] {-666, 3}).stream())
                    .map(ar -> (Tuple) Row.of(b, posRight.getAndIncrement(), ar))
                    .iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_one_to_many()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
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

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(19, count);
    }

    @Test
    public void test_inner_join_many_to_one()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 5);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> rows = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return rows.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_many_to_one()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> (Tuple) Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(24, count);
    }

    @Test
    public void test_inner_join_many_to_many()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            // Create 2 rows for each input row
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .collect(toList());

            return inner.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(20, count);
    }

    @Test
    public void test_outer_join_many_to_many()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            // Create 2 rows for each input row
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .flatMap(val -> asList(new Object[] {val, 1}, new Object[] {val, 2}, new Object[] {val, 3}).stream())
                    .map(ar -> Row.of(b, posRight.getAndIncrement(), ar))
                    .collect(toList());

            return inner.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1 and b.col2 < 3")),
                DefaultTupleMerger.DEFAULT,
                false,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(expectedInnerPositions[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(34, count);
    }

    @Test
    public void test_inner_join_one_to_one_populating()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                false,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);
            assertArrayEquals(new int[] {expectedInnerPositions[count]}, col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_one_populating()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
            Iterable<Object[]> it = () -> context.getOperatorContext().getOuterIndexValues();
            List<Tuple> inner = StreamSupport.stream(it.spliterator(), false)
                    .map(ar -> (Integer) ar[0])
                    .filter(val -> val >= 5 && val <= 9)
                    .distinct()
                    .map(val -> Row.of(b, posRight.getAndIncrement(), new Object[] {val, "Val" + val}))
                    .collect(toList());

            return inner.iterator();
        });

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).collect(toList()) : null);
            count++;
        }

        assertEquals(14, count);
    }

    @Test
    public void test_inner_join_one_to_many_populating()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 2);
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

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                false,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);

            assertArrayEquals("" + count, expectedInnerPositions[count], col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            count++;
        }

        assertEquals(5, count);
    }

    @Test
    public void test_outer_join_one_to_many_populating()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 2);
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

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).collect(toList()) : null);
            count++;
        }

        assertEquals(14, count);
    }

    @Test
    public void test_inner_join_many_to_many_populating()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
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

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                false,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);

            assertArrayEquals("" + count, expectedInnerPositions[count], col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_many_to_many_populating()
    {
        Index index = new Index(QualifiedName.of("table"), asList("col"), 3);
        MutableInt posLeft = new MutableInt();
        MutableInt posRight = new MutableInt();
        Operator left = op(context -> asList(-2, -1, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 11)
                .stream()
                .map(i -> (Tuple) Row.of(a, posLeft.getAndIncrement(), new Object[] {i}))
                .iterator());
        Operator right = op(context ->
        {
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

        BatchHashJoin op = new BatchHashJoin(0,
                "",
                left,
                right,
                new ExpressionValuesExtractor(asList(e("a.col1"))),
                new ExpressionValuesExtractor(asList(e("b.col1"))),
                new ExpressionPredicate(e("a.col1 > 0 and b.col1 = a.col1")),
                //                (ctx, row, values) -> values[0] = row.getObject(0),
                //                (ctx, row, values) -> values[0] = row.getObject(0),
                //                (ctx, row) -> (Integer) row.getParent().getObject(0) > 0 && (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
                DefaultTupleMerger.DEFAULT,
                true,
                true,
                index, null);

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
            assertEquals(expectedOuterPositions[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);

            assertEquals("" + count, expectedInnerPositions.get(count), col != null ? col.stream().map(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).collect(toList()) : null);
            count++;
        }

        assertEquals(24, count);
    }

    //    @Ignore
    //    @Test
    //    public void test_inner_join_large()
    //    {
    //        Index index = new Index(QualifiedName.of("table"), asList("col"), 250);
    //        Random rnd = new Random();
    //        TableAlias a = TableAlias.of(null, "tableA", "a");
    //        TableAlias b = TableAlias.of(a, "tableB", "b");
    //        TableAlias c = TableAlias.of(b, "tableC", "c");
    //        MutableInt bPos = new MutableInt();
    //        MutableInt cPos = new MutableInt();
    //
    //        Operator tableA = op(context -> IntStream.range(1, 1000000).mapToObj(i -> Row.of(a, i, new Object[] {rnd.nextInt(10000), rnd.nextBoolean()})).iterator());
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
    //            return StreamSupport.stream(it.spliterator(), false)
    //                    .map(ar -> (Integer) ar[0])
    //                    .flatMap(val -> asList(
    //                            new Object[] {val, 1, "Val" + val},
    //                            new Object[] {val, 4, rnd.nextBoolean()}).stream())
    //                    .map(ar -> Row.of(c, cPos.getAndIncrement(), ar))
    //                    .iterator();
    //        });
    //
    //        BatchHashJoin op = new BatchHashJoin(0,
    //                "",
    //                tableA,
    //                new BatchHashJoin(0,
    //                        "",
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
    //                        index),
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
    //                index, null);
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
    //                //                                                                System.out.println(row + " " + row.getChildRows(0).stream().map(r -> r.toString() + " " + r.getChildRows(0)).collect(joining(", ")) );
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
