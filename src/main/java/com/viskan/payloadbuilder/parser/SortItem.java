package com.viskan.payloadbuilder.parser;

import static java.util.Objects.requireNonNull;

public class SortItem extends ASelectNode
{
    private final Expression expression;
    private final Order order;
    private final NullOrder nullOrder;
    
    public SortItem(Expression expression, Order order, NullOrder nullOrder)
    {
        this.expression = requireNonNull(expression, "expression");
        this.order = requireNonNull(order, "order");
        this.nullOrder = requireNonNull(nullOrder, "nullOrder");
    }
    
    public Expression getExpression()
    {
        return expression;
    }
    
    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return expression + " " + order + (nullOrder != NullOrder.UNDEFINED ? (" NULLS " + nullOrder) : "");
    }
    
    public enum NullOrder
    {
        FIRST,LAST,UNDEFINED;
    }
    
    public enum Order
    {
        ASC,DESC;
    }
}
