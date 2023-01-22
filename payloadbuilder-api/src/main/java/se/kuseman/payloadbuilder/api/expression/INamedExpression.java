package se.kuseman.payloadbuilder.api.expression;

/** Definition of a named expression */
public interface INamedExpression extends IUnaryExpression
{
    /** Return the name of the expression */
    String getName();
}
