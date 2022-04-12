package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.core.parser.SortItem;

/** Test of {@link ExpressionTupleComparator} */
public class ExpressionTupleComparatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
                .tableMeta(new TableMeta(asList(new TableMeta.Column("col1", DataType.ANY))))
                .build();
        Row a;
        Row b;

        a = Row.of(alias, new Object[] { 1 });
        b = Row.of(alias, new Object[] { 2 });

        List<ISortItem> items = new ArrayList<>(asList(new SortItem(e("col1"), Order.ASC, NullOrder.UNDEFINED, null)));

        ExecutionContext context = new ExecutionContext(session);
        ExpressionTupleComparator comparator = new ExpressionTupleComparator(items);
        assertEquals(-1, comparator.compare(context, a, b));

        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED, null));
        assertEquals(1, comparator.compare(context, a, b));

        a = Row.of(alias, new Object[] { 2 });
        assertEquals(0, comparator.compare(context, a, b));

        a = Row.of(alias, new Object[] { null });
        b = Row.of(alias, new Object[] { null });

        assertEquals(0, comparator.compare(context, a, b));

        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST, null));
        a = Row.of(alias, new Object[] { 1 });
        b = Row.of(alias, new Object[] { null });
        assertEquals(1, comparator.compare(context, a, b));

        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST, null));
        a = Row.of(alias, new Object[] { 1 });
        b = Row.of(alias, new Object[] { null });
        assertEquals(-1, comparator.compare(context, a, b));

        // Nulls first
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.FIRST, null));
        a = Row.of(alias, new Object[] { null });
        b = Row.of(alias, new Object[] { 1 });
        assertEquals(-1, comparator.compare(context, a, b));

        // Nulls last
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.LAST, null));
        a = Row.of(alias, new Object[] { null });
        b = Row.of(alias, new Object[] { 1 });
        assertEquals(1, comparator.compare(context, a, b));

        // Nulls first if undefined
        items.clear();
        items.add(new SortItem(e("col1"), Order.DESC, NullOrder.UNDEFINED, null));
        a = Row.of(alias, new Object[] { null });
        b = Row.of(alias, new Object[] { 1 });
        assertEquals(-1, comparator.compare(context, a, b));
        a = Row.of(alias, new Object[] { 1 });
        b = Row.of(alias, new Object[] { null });
        assertEquals(1, comparator.compare(context, a, b));
    }
}
