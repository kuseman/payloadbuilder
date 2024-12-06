package se.kuseman.payloadbuilder.api.expression;

/** A nested (parenthesis) expression */
public interface INestedExpression extends IUnaryExpression
{
    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
