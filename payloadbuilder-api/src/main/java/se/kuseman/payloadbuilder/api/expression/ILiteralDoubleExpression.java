package se.kuseman.payloadbuilder.api.expression;

/** Literal double */
public interface ILiteralDoubleExpression extends ILiteralExpression
{
    /** Get value */
    double getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
