package se.kuseman.payloadbuilder.api.expression;

/** Logical not expression */
public interface ILogicalNotExpression extends IUnaryExpression
{
    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
