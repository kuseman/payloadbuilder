package se.kuseman.payloadbuilder.api.expression;

/** Definition of a named expression */
public interface INamedExpression extends IExpression
{
    /** Return the name of the expression */
    String getName();

    /** Return the expression associated with the name */
    IExpression getExpression();
}
