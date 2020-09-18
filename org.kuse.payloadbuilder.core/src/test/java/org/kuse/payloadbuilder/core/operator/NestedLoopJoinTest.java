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
import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test {@link NestedLoopJoin} */
public class NestedLoopJoinTest extends AOperatorTest
{
    private final TableAlias a = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA"), "a")
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
    public void test_correlated()
    {
        // Test that a correlated query with a nestedloop
        // uses the context row into consideration when joining

        /**
         * <pre>
         * from tableA a
         * inner join
         * [
         *   tableB b
         *   inner join [tableC] c
         *      on c.id2 = a.id2
         *      or c.id = b.id
         * ] b
         *   on b.id = a.id
         * </pre>
         */

        Operator opA = op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> (Tuple) Row.of(a, i, new Object[] {i, "val" + i})).iterator());
        Operator opB = op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(b, 10 * i, new Object[] {i})).iterator());
        Operator opC = op(ctx -> IntStream.of(1, 2, 3, 4, 5, 6, 7).mapToObj(i -> (Tuple) Row.of(c, 100 * i, new Object[] {i, "val" + i})).iterator());

        Operator op = new NestedLoopJoin(
                0,
                "",
                opA,
                new NestedLoopJoin(
                        1,
                        "",
                        opB,
                        opC,
                        new ExpressionPredicate(e("col2 = a.col2 or c.col1 = b.col1")),
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
    public void test_cross_join_no_populate()
    {
        MutableBoolean leftClose = new MutableBoolean();
        MutableBoolean rightClose = new MutableBoolean();
        Operator left = op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.setTrue());
        Operator right = op(ctx -> new TableFunctionOperator(0, "", b, new Range(2), emptyList()).open(ctx), () -> rightClose.setTrue());
        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                right,
                null,   // Null predicate => cross
                DefaultTupleMerger.DEFAULT,
                false,
                false);

        RowIterator it = op.open(new ExecutionContext(session));

        int[] tableAPos = new int[] {0, 0, 1, 1, 2, 2, 3, 3};
        int[] tableBPos = new int[] {0, 1, 0, 1, 0, 1, 0, 1};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(tableBPos[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }
        it.close();
        assertFalse(it.hasNext());
        assertEquals(8, count);
        assertTrue(leftClose.booleanValue());
        assertTrue(rightClose.booleanValue());
    }

    @Test
    public void test_cross_join_populate()
    {
        Operator left = op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(2), emptyList()),
                null,   // Null predicate => cross
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
            assertArrayEquals(new int[] {0, 1}, col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            count++;
        }

        assertEquals(4, count);
    }

    @Test
    public void test_outer_join()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(0), emptyList()),
                null,   // Null predicate => cross
                DefaultTupleMerger.DEFAULT,
                false,
                true);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            assertEquals(count, tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            // Outer apply, no b rows should be present
            assertNull(tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        assertEquals(9, count);
    }

    @Test
    public void test_outer_join_with_predicate_no_populate()
    {
        Operator left = op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(2), emptyList()),
                // Even parent rows are joined
                (ctx, tuple) -> (int) tuple.getValue(QualifiedName.of("a", "__pos"), 0) % 2 == 0,
                DefaultTupleMerger.DEFAULT,
                false,
                true);

        RowIterator it = op.open(new ExecutionContext(session));

        Integer[] tableAPos = new Integer[] {0, 0, 1, 2, 2, 3};
        Integer[] tableBPos = new Integer[] {0, 1, null, 0, 1, null};

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(tableAPos[count], tuple.getValue(QualifiedName.of("a", "__pos"), 0));
            assertEquals(tableBPos[count], tuple.getValue(QualifiedName.of("b", "__pos"), 0));
            count++;
        }

        // 4 joined rows and 2 non joined
        assertEquals(6, count);
    }

    @Test
    public void test_outer_join_with_predicate_populate()
    {
        Operator left = op(context -> IntStream.range(1, 5).mapToObj(i -> (Tuple) Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(2), emptyList()),
                // Even parent rows are joined
                (ctx, tuple) -> (int) tuple.getValue(QualifiedName.of("a", "__pos"), 0) % 2 == 0,
                DefaultTupleMerger.DEFAULT,
                true,
                true);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) tuple.getValue(QualifiedName.of("a", "__pos"), 0);
            assertEquals(count, pos);
            @SuppressWarnings("unchecked")
            Collection<Tuple> col = (Collection<Tuple>) tuple.getValue(QualifiedName.of("b"), 0);
            if (pos % 2 == 0)
            {
                assertArrayEquals(new int[] {0, 1}, col.stream().mapToInt(t -> (int) t.getValue(QualifiedName.of("__pos"), 0)).toArray());
            }
            else
            {
                assertNull(col);
            }

            count++;
        }

        assertEquals(4, count);
    }

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
