package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.catalog.ISortItem;

/** Sort item */
public class SortItem extends ASelectNode implements ISortItem
{
    private final Expression expression;
    private final Order order;
    private final NullOrder nullOrder;
    private final Token token;

    public SortItem(Expression expression, Order order, NullOrder nullOrder, Token token)
    {
        this.expression = requireNonNull(expression, "expression");
        this.order = requireNonNull(order, "order");
        this.nullOrder = requireNonNull(nullOrder, "nullOrder");
        this.token = token;
    }

    @Override
    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public Order getOrder()
    {
        return order;
    }

    @Override
    public NullOrder getNullOrder()
    {
        return nullOrder;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SortItem)
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
        return expression + " "
               + order
               + (nullOrder != NullOrder.UNDEFINED ? (" NULLS " + nullOrder)
                       : "");
    }
}
