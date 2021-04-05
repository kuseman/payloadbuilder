package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Test of {@link ExpressionTupleComparator} */
public class ExpressionTupleComparatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("table"), "a").columns(new String[] {"col1"}).build();
        Row a, b;

        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {2});

        List<SortItem> items = new ArrayList<>(asList(
                new SortItem(e("col1"), Order.ASC, NullOrder.UNDEFINED, null)));

        ExecutionContext context = new ExecutionContext(session);
        ExpressionTupleComparator comparator = new ExpressionTupleComparator(items);
        assertEquals(-1, comparator.compare(context, a, b));

        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED, null));
        assertEquals(1, comparator.compare(context, a, b));

        a = Row.of(alias, 0, new Object[] {2});
        assertEquals(0, comparator.compare(context, a, b));

        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {null});

        assertEquals(0, comparator.compare(context, a, b));

        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST, null));
        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {null});
        assertEquals(1, comparator.compare(context, a, b));

        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST, null));
        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {null});
        assertEquals(-1, comparator.compare(context, a, b));

        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST, null));
        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {1});
        assertEquals(-1, comparator.compare(context, a, b));

        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST, null));
        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {1});
        assertEquals(1, comparator.compare(context, a, b));

        // Nulls first if undefined
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED, null));
        a = Row.of(alias, 0, new Object[] {null});
        b = Row.of(alias, 0, new Object[] {1});
        assertEquals(-1, comparator.compare(context, a, b));
        a = Row.of(alias, 0, new Object[] {1});
        b = Row.of(alias, 0, new Object[] {null});
        assertEquals(1, comparator.compare(context, a, b));
    }
}
