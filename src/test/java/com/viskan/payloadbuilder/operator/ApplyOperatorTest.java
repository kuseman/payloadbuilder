package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.ApplyOperator.Type;

import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/** Test {@link ApplyOperator} */
public class ApplyOperatorTest extends Assert
{
    @Test
    public void test_cross_with_plain_join()
    {
        TableAlias a = TableAlias.of(null, "table", "t");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator();
        ApplyOperator op = new ApplyOperator(left, Type.CROSS, new Range(2), emptyList(), RowMerger.COPY);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            assertEquals((count % 2) + 1, row.getChildRows(0).get(0).getObject(0));
            count++;
        }

        assertEquals(18, count);
    }
    
    @Test
    public void test_cross_with_populating_join()
    {
        TableAlias a = TableAlias.of(null, "table", "t");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator();
        ApplyOperator op = new ApplyOperator(left, Type.CROSS, new Range(2), emptyList(), RowMerger.DEFAULT);

        Iterator<Row> it = new Distinct(op).open(new OperatorContext());
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
    public void test_outer()
    {
        TableAlias a = TableAlias.of(null, "table", "t");
        Operator left = context -> IntStream.range(1, 10).mapToObj(i -> Row.of(a, i - 1, new Object[] {i})).iterator();
        ApplyOperator op = new ApplyOperator(left, Type.OUTER, new Range(0), emptyList(), RowMerger.DEFAULT);

        Iterator<Row> it = op.open(new OperatorContext());
        int count = 0;
        while (it.hasNext())
        {
            Row row = it.next();
            // Outer apply, child rows should be null
            assertNull(row.getChildRows(0));
            count++;
        }

        assertEquals(9, count);
    }

    private static class Range extends TableFunctionInfo
    {
        private final int to;

        Range(int to)
        {
            super(new Catalog("test"), "Range");
            this.to = to;
        }

        @Override
        public Iterator<Row> open(OperatorContext context, TableAlias tableAlias, List<Object> arguments)
        {
            if (tableAlias.getColumns() == null)
            {
                tableAlias.setColumns(new String[] { "Value" });
            }
            return IntStream.range(0, to).mapToObj(i -> Row.of(tableAlias, i, new Object[] { i + 1 })).iterator();
        }
    }
}
