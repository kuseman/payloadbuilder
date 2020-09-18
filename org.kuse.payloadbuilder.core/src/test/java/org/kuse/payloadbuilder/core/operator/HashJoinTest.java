/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test {@link HashJoin} */
public class HashJoinTest extends AOperatorTest
{
    private final TableAlias a = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA"), "a")
            .columns(new String[] {"col1", "col2"})
            .children(asList(
                    TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableB"), "b")
                            .columns(new String[] {"col1"})
                            .children(asList(
                                    TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableC"), "c")
                                            .columns(new String[] {"col1", "col2"})))))

            .build();
    private final TableAlias b = a.getChildAliases().get(0);
    private final TableAlias c = b.getChildAliases().get(0);

    @Test
    public void test_inner_join_no_populate_empty()
    {
        HashJoin op = new HashJoin(0,
                "",
                op(c -> emptyIterator()),
                op(c -> emptyIterator()),
                (c, tuple) -> 0,
                (c, tuple) -> 0,
                (ctx, row) -> false,
                DefaultTupleMerger.DEFAULT,
                false,
                false);
        RowIterator it = op.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_no_populate()
    {
        MutableBoolean leftClose = new MutableBoolean();
        MutableBoolean rightClose = new MutableBoolean();
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.setTrue());
        Operator right = op(ctx -> new TableFunctionOperator(0, "", b, new NestedLoopJoinTest.Range(5), emptyList()).open(ctx), () -> rightClose.setTrue());
        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                new ExpressionHashFunction(asList(e("a.col1"))),
                new ExpressionHashFunction(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] tableAPos = new int[] {1, 2, 3, 4, 5};
        int[] tableBPos = new int[] {1, 2, 3, 4, 5};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(tableAPos[count], tuple.getValue(QualifiedName.of("a", "col1"), 0));
            assertEquals(tableBPos[count], tuple.getValue(QualifiedName.of("b", "col1"), 0));
            count++;
        }
        it.close();
        assertFalse(it.hasNext());
        assertEquals(5, count);
        assertTrue(leftClose.booleanValue());
        assertTrue(rightClose.booleanValue());
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

        Operator opA = op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i, "val" + i})).iterator());
        Operator opB = op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(b, i * 10, new Object[] {i})).iterator());
        Operator opC = op(ctx -> IntStream.of(1, 2, 3, 4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(c, i * 100, new Object[] {i, "val" + i})).iterator());

        Operator op = new NestedLoopJoin(
                0,
                "",
                opA,
                new HashJoin(
                        1,
                        "",
                        opB,
                        opC,
                        new ExpressionHashFunction(asList(e("b.col1"))),
                        new ExpressionHashFunction(asList(e("c.col1"))),
                        new ExpressionPredicate(e("c.col1 = b.col1 and c.col2 = a.col2")),
                        DefaultTupleMerger.DEFAULT,
                        false,
                        false),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                false);

        int[] tableAPos = new int[] {4, 5};
        int[] tableBPos = new int[] {40, 50};
        int[] tableCPos = new int[] {400, 500};

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
    public void test_inner_join_populate()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator());
        Operator right = op(context -> IntStream.range(1, 20).mapToObj(i -> (Tuple) Row.of(b, i - 1, new Object[] {i % 10})).iterator());
        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                new ExpressionHashFunction(asList(e("a.col1"))),
                new ExpressionHashFunction(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                false);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(count, tuple.getValue(QualifiedName.of("a", "__pos"), 0));

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);
            assertArrayEquals(new int[] {count + 0, count + 10}, col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            count++;
        }
        assertFalse(it.hasNext());
        assertEquals(9, count);
    }

    @Test
    public void test_outer_join_no_populate()
    {
        Operator left = op(context -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context -> IntStream.range(5, 15).mapToObj(i -> (Tuple) Row.of(b, i, new Object[] {i})).iterator());

        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                new ExpressionHashFunction(asList(e("a.col1"))),
                new ExpressionHashFunction(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                false,
                true);

        RowIterator it = op.open(new ExecutionContext(session));

        Integer[] tableAPos = new Integer[] {5, 6, 7, 8, 9, 0, 1, 2, 3, 4};
        Integer[] tableBPos = new Integer[] {5, 6, 7, 8, 9, null, null, null, null, null};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(tableBPos[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(10, count);
    }

    @Test
    public void test_outer_join_populate()
    {
        Operator left = op(context -> IntStream.range(0, 5).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i})).iterator());
        Operator right = op(context -> IntStream.range(5, 15).mapToObj(i -> (Tuple) Row.of(b, i - 1, new Object[] {i % 5 + 2})).iterator());

        HashJoin op = new HashJoin(0,
                "",
                left,
                right,
                new ExpressionHashFunction(asList(e("a.col1"))),
                new ExpressionHashFunction(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                DefaultTupleMerger.DEFAULT,
                true,
                true);

        RowIterator it = op.open(new ExecutionContext(session));

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
            int pos = (int) tuple.getValue(QualifiedName.of("a", "__pos"), 0);
            assertEquals(count, pos);

            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);

            if (pos >= 2)
            {
                assertArrayEquals(tableBPos[count - 2], col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            }
            else
            {
                assertNull(col);
            }
            count++;
        }

        assertEquals(5, count);
    }
}
