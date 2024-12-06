package se.kuseman.payloadbuilder.api.expression;

/** An explicit cast expression */
public interface ICastExpression extends IUnaryExpression
{
    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
