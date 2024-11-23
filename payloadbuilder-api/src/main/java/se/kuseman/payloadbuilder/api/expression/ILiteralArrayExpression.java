package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Literal array */
public interface ILiteralArrayExpression extends ILiteralExpression
{
    /** Return the Array value */
    ValueVector getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
