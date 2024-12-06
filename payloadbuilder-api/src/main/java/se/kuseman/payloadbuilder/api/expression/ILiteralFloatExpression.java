package se.kuseman.payloadbuilder.api.expression;

/** Literal long */
public interface ILiteralFloatExpression extends ILiteralExpression
{
    /** Get value */
    float getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
