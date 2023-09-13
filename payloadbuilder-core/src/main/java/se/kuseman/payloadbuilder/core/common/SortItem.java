package se.kuseman.payloadbuilder.core.common;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Definition of a sort item */
public class SortItem implements ISortItem
{
    private final IExpression expression;
    private final Order order;
    private final NullOrder nullOrder;
    private final Location location;

    public SortItem(IExpression expression, Order order, NullOrder nullOrder, Location location)
    {
        this.expression = requireNonNull(expression, "expression");
        this.order = requireNonNull(order, "order");
        this.nullOrder = requireNonNull(nullOrder, "nullOrder");
        this.location = location;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public NullOrder getNullOrder()
    {
        return nullOrder;
    }

    @Override
    public Order getOrder()
    {
        return order;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof SortItem)
        {
            SortItem that = (SortItem) obj;
            return expression.equals(that.expression)
                    && order == that.order
                    && nullOrder == that.nullOrder;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression.toVerboseString() + " " + order;
    }
}