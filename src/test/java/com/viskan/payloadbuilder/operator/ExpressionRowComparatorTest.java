package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.SortItem;
import com.viskan.payloadbuilder.parser.SortItem.NullOrder;
import com.viskan.payloadbuilder.parser.SortItem.Order;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/** Test of {@link ExpressionRowComparator} */
public class ExpressionRowComparatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = new TableAlias(null, QualifiedName.of("table"), "a", new String[] {"col1"});
        Row a,b;
        
        a = Row.of(alias, 0, new Object[] { 1 });
        b = Row.of(alias, 0, new Object[] { 2 });
        
        List<SortItem> items = new ArrayList<>(asList(
                new SortItem(e("col1"), Order.ASC, NullOrder.UNDEFINED)
                ));
        
        ExecutionContext context = new ExecutionContext(session);
        ExpressionRowComparator comparator = new ExpressionRowComparator(items);
        assertEquals(-1, comparator.compare(context, a, b));

        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED));
        assertEquals(1, comparator.compare(context, a, b));
        
        a =  Row.of(alias, 0, new Object[] { 2 });
        assertEquals(0, comparator.compare(context, a, b));
        
        a = Row.of(alias, 0, new Object[] { null });
        b = Row.of(alias, 0, new Object[] { null });
        
        assertEquals(0, comparator.compare(context, a, b));
        
        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST));
        a = Row.of(alias, 0, new Object[] { 1 });
        b = Row.of(alias, 0, new Object[] { null });
        assertEquals(1, comparator.compare(context, a, b));
        
        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST));
        a = Row.of(alias, 0, new Object[] { 1 });
        b = Row.of(alias, 0, new Object[] { null });
        assertEquals(-1, comparator.compare(context, a, b));
        
        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST));
        a = Row.of(alias, 0, new Object[] { null });
        b = Row.of(alias, 0, new Object[] { 1 });
        assertEquals(-1, comparator.compare(context, a, b));
        
        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST));
        a = Row.of(alias, 0, new Object[] { null });
        b = Row.of(alias, 0, new Object[] { 1 });
        assertEquals(1, comparator.compare(context, a, b));
        
        // Nulls first if undefined
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED));
        a = Row.of(alias, 0, new Object[] { null });
        b = Row.of(alias, 0, new Object[] { 1 });
        assertEquals(-1, comparator.compare(context, a, b));
        a = Row.of(alias, 0, new Object[] { 1 });
        b = Row.of(alias, 0, new Object[] { null });
        assertEquals(1, comparator.compare(context, a, b));
        
    }
}
