package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.ExpressionMath;
import com.viskan.payloadbuilder.parser.SortItem;
import com.viskan.payloadbuilder.parser.SortItem.NullOrder;
import com.viskan.payloadbuilder.parser.SortItem.Order;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Interpreter based row comparator */
class ExpressionRowComparator implements RowComparator
{
    private final List<SortItem> items;

    ExpressionRowComparator(List<SortItem> items)
    {
        this.items = requireNonNull(items);
    }

    @Override
    public int compare(ExecutionContext context, Row a, Row b)
    {
        int size = items.size();

        for (int i = 0; i < size; i++)
        {
            SortItem item = items.get(i);
            NullOrder nullOrder = item.getNullOrder();
            Order order = item.getOrder();
            Expression e = item.getExpression();

            context.setRow(a);
            Object aValue = e.eval(context);
            context.setRow(b);
            Object bValue = e.eval(context);

            if (aValue == null && bValue == null)
            {
                continue;
            }
            else if (aValue == null)
            {
                // Null is always less if not specified
                return nullOrder == NullOrder.FIRST
                    || nullOrder == NullOrder.UNDEFINED ? -1 : 1;
            }
            else if (bValue == null)
            {
                // Null is always less if not specified
                return nullOrder == NullOrder.FIRST
                    || nullOrder == NullOrder.UNDEFINED ? 1 : -1;
            }

            int c = ExpressionMath.cmp(aValue, bValue);
            if (c != 0)
            {
                return c * (order == Order.DESC ? -1 : 1);
            }
        }

        return 0;
    }

    @Override
    public int hashCode()
    {
        return items.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionRowComparator)
        {
            ExpressionRowComparator that = (ExpressionRowComparator) obj;
            return items.equals(that.items);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return items.toString();
    }

}
