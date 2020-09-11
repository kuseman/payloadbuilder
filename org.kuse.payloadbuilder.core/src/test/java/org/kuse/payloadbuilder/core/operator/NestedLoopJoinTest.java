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

import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test {@link NestedLoopJoin} */
public class NestedLoopJoinTest extends AOperatorTest
{
    private final TableAlias a = TableAlias.of(null, QualifiedName.of("table"), "a");
    private final TableAlias b = TableAlias.of(a, QualifiedName.of("tableB"), "b");

    @Test
    public void test_correlated()
    {
        // Test that a correlated query with a nestedloop
        // uses the context row into consideration when joining

        TableAlias c = TableAlias.of(b, QualifiedName.of("tableC"), "c");

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

        Operator opA = op(ctx -> IntStream.of(1, 2, 3, 4, 5).mapToObj(i -> Row.of(a, i, new Object[] {i, "val" + i})).iterator());
        Operator opB = op(ctx -> IntStream.of(4, 5, 6, 7).mapToObj(i -> Row.of(b, i, new Object[] {i})).iterator());
        Operator opC = op(ctx -> IntStream.of(1, 2, 3, 4, 5, 6, 7).mapToObj(i -> Row.of(c, i, new Object[] {i, "val" + i})).iterator());

        Operator op = new NestedLoopJoin(
                0,
                "",
                opA,
                new NestedLoopJoin(
                        1,
                        "",
                        opB,
                        opC,
                        (ctx, row) -> Objects.equals(row.getObject(1), row.getParent().getParent().getObject(1))
                            || (Integer) row.getObject(0) == (Integer) row.getParent().getObject(0),
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
    public void test_cross_join_no_populate()
    {
        MutableBoolean leftClose = new MutableBoolean();
        MutableBoolean rightClose = new MutableBoolean();
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator(), () -> leftClose.setTrue());
        Operator right = op(ctx -> new TableFunctionOperator(0, "", b, new Range(2), emptyList()).open(ctx), () -> rightClose.setTrue());
        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                right,
                null,   // Null predicate => cross
                DefaultRowMerger.DEFAULT,
                false,
                false);

        RowIterator it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals((count % 2) + 1, row.getChildRows(0).get(0).getObject(0));
            count++;
        }
        it.close();
        assertFalse(it.hasNext());
        assertEquals(18, count);
        assertTrue(leftClose.booleanValue());
        assertTrue(rightClose.booleanValue());
    }

    @Test
    public void test_cross_join_populate()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(2), emptyList()),
                null,   // Null predicate => cross
                DefaultRowMerger.DEFAULT,
                true,
                false);

        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals(1, row.getChildRows(0).get(0).getObject(0));
            assertEquals(2, row.getChildRows(0).get(1).getObject(0));
            count++;
        }

        assertEquals(9, count);
    }

    @Test
    public void test_outer_join()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(0), emptyList()),
                null,   // Null predicate => cross
                DefaultRowMerger.DEFAULT,
                false,
                true);

        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            // Outer apply, child rows should be empty
            assertEquals(emptyList(), row.getChildRows(0));
            count++;
        }

        assertEquals(9, count);
    }

    @Test
    public void test_outer_join_with_predicate_no_populate()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(2), emptyList()),
                // Even parent rows are joined
                (ctx, row) -> row.getParent().getPos() % 2 == 0,
                DefaultRowMerger.DEFAULT,
                false,
                true);

        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        int subCount = 1;
        while (it.hasNext())
        {
            Row row = it.next();
            if (row.getPos() % 2 == 0)
            {
                assertEquals(subCount++, row.getChildRows(0).get(0).getObject(0));
                if (subCount >= 3)
                {
                    subCount = 1;
                }
            }
            else
            {
                assertEquals(emptyList(), row.getChildRows(0));
            }

            count++;
        }

        // 9 joined rows and 5 non joined
        assertEquals(14, count);
    }

    @Test
    public void test_outer_join_with_predicate_populate()
    {
        Operator left = op(context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator());

        NestedLoopJoin op = new NestedLoopJoin(
                0,
                "",
                left,
                new TableFunctionOperator(0, "", b, new Range(2), emptyList()),
                // Even parent rows are joined
                (ctx, row) -> row.getParent().getPos() % 2 == 0,
                DefaultRowMerger.DEFAULT,
                true,
                true);

        Iterator<Row> it = op.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            if (row.getPos() % 2 == 0)
            {
                assertEquals(1, row.getChildRows(0).get(0).getObject(0));
                assertEquals(2, row.getChildRows(0).get(1).getObject(0));
            }
            else
            {
                assertEquals(emptyList(), row.getChildRows(0));
            }

            count++;
        }

        assertEquals(9, count);
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
            return RowIterator.wrap(IntStream.range(0, to).mapToObj(i -> Row.of(tableAlias, i, new Object[] {i + 1})).iterator());
        }
    }
}
