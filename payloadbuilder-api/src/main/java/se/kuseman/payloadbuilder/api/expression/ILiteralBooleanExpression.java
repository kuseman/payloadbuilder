package se.kuseman.payloadbuilder.api.expression;

/** Literal boolean */
public interface ILiteralBooleanExpression extends ILiteralExpression
{
    /** Get value */
    boolean getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
