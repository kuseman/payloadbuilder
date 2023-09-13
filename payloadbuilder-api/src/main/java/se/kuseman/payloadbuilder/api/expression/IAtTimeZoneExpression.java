package se.kuseman.payloadbuilder.api.expression;

/** At timezone function */
public interface IAtTimeZoneExpression extends IExpression
{
    /** Return expression */
    IExpression getExpression();

    /** Return time zone expression */
    IExpression getTimeZone();
}
