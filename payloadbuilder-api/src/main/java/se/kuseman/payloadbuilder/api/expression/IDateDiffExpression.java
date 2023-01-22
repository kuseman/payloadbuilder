package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.expression.IDatePartExpression.Part;

/** Date diff function */
public interface IDateDiffExpression extends IExpression
{
    /** Return date part */
    Part getPart();

    /** Return the start date expression */
    IExpression getStart();

    /** Return the end date expression */
    IExpression getEnd();
}
