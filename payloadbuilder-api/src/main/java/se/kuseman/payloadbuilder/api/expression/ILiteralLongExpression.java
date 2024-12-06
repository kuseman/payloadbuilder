package se.kuseman.payloadbuilder.api.expression;

/** Literal long */
public interface ILiteralLongExpression extends ILiteralExpression
{
    /** Get value */
    long getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
