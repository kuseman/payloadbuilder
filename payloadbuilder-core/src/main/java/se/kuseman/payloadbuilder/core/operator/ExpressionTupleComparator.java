package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Interpreter based row comparator */
class ExpressionTupleComparator implements TupleComparator
{
    private final List<ISortItem> items;

    ExpressionTupleComparator(List<ISortItem> items)
    {
        this.items = requireNonNull(items);
    }

    @Override
    public int compare(ExecutionContext context, Tuple a, Tuple b)
    {
        int size = items.size();

        for (int i = 0; i < size; i++)
        {
            ISortItem item = items.get(i);
            NullOrder nullOrder = item.getNullOrder();
            Order order = item.getOrder();
            IExpression e = item.getExpression();

            context.getStatementContext()
                    .setTuple(a);
            Object valueA = e.eval(context);
            context.getStatementContext()
                    .setTuple(b);
            Object valueB = e.eval(context);

            if (valueA == null
                    && valueB == null)
            {
                continue;
            }
            else if (valueA == null)
            {
                // Null is always less if not specified
                return nullOrder == NullOrder.FIRST
                        || nullOrder == NullOrder.UNDEFINED ? -1
                                : 1;
            }
            else if (valueB == null)
            {
                // Null is always less if not specified
                return nullOrder == NullOrder.FIRST
                        || nullOrder == NullOrder.UNDEFINED ? 1
                                : -1;
            }

            int c = ExpressionMath.cmp(valueA, valueB);
            if (c != 0)
            {
                return c * (order == Order.DESC ? -1
                        : 1);
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
        if (obj instanceof ExpressionTupleComparator)
        {
            ExpressionTupleComparator that = (ExpressionTupleComparator) obj;
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
