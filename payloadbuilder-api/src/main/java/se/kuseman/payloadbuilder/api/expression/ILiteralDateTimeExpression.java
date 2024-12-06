package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.EpochDateTime;

/** Literal date time */
public interface ILiteralDateTimeExpression extends ILiteralExpression
{
    /** Get value */
    EpochDateTime getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
