package se.kuseman.payloadbuilder.api.expression;

/** At timezone function */
public interface IAtTimeZoneExpression extends IExpression
{
    /** Return expression */
    IExpression getExpression();

    /** Return time zone expression */
    IExpression getTimeZone();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
