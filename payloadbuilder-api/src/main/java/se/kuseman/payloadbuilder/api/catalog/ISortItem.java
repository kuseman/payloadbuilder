package se.kuseman.payloadbuilder.api.catalog;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Definition of a sort item */
public interface ISortItem
{
    /** Get order */
    Order getOrder();

    /** Get null order */
    NullOrder getNullOrder();

    /** Return the expression for this sort item */
    IExpression getExpression();

    /** Null order type */
    public enum NullOrder
    {
        FIRST,
        LAST,
        UNDEFINED;
    }

    /** Order direction */
    public enum Order
    {
        ASC,
        DESC;
    }
}
