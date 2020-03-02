package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public class SortItem
{
    private final Expression sort;
    private final Order order;
    private final NullOrder nullOrder;
    
    public SortItem(Expression sort, Order order, NullOrder nullOrder)
    {
        this.sort = requireNonNull(sort, "sort");
        this.order = requireNonNull(order, "order");
        this.nullOrder = requireNonNull(nullOrder, "nullOrder");
    }
    
    public enum NullOrder
    {
        FIRST,LAST,UNDEFINED;
    }
    
    public enum Order
    {
        ASC,DESC;
    }
    
    @Override
    public String toString()
    {
        return sort + " " + order + (nullOrder != NullOrder.UNDEFINED ? (" NULLS " + nullOrder) : "");
    }
}
