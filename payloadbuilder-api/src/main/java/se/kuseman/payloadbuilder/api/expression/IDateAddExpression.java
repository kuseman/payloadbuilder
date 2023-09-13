package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.expression.IDatePartExpression.Part;

/** Date add function */
public interface IDateAddExpression extends IExpression
{
    /** Return date part */
    Part getPart();

    /** Return the date expression */
    IExpression getExpression();

    /** Return number expression */
    IExpression getNumber();
}
