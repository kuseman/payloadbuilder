package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Literal stringILiteralDoubleExpression.java */
public interface ILiteralStringExpression extends ILiteralExpression
{
    /** Get value */
    UTF8String getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
