package se.kuseman.payloadbuilder.api.expression;

/** Literal integer */
public interface ILiteralIntegerExpression extends ILiteralExpression
{
    /** Get value */
    int getValue();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
