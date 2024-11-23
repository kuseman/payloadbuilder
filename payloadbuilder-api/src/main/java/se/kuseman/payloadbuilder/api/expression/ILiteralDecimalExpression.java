package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.Decimal;

/** Literal decimal */
public interface ILiteralDecimalExpression extends ILiteralExpression
{
    /** Get value */
    Decimal getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
