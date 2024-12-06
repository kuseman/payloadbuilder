package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.ObjectVector;

/** Literal object */
public interface ILiteralObjectExpression extends ILiteralExpression
{
    /** Return the Object value */
    ObjectVector getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
