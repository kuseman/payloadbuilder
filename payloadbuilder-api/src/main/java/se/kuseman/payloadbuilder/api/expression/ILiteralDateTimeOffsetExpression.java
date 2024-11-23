package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;

/** Literal date time offset */
public interface ILiteralDateTimeOffsetExpression extends ILiteralExpression
{
    /** Get value */
    EpochDateTimeOffset getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
