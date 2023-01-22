package se.kuseman.payloadbuilder.api.expression;

/** Subscript. expr[0] */
public interface ISubscriptExpression extends IExpression
{
    /** Return value expression */
    IExpression getValue();

    /** Return sub script expression */
    IExpression getSubscript();
}
