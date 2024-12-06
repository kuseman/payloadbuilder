package se.kuseman.payloadbuilder.api.expression;

/** Logical binary expression */
public interface ILogicalBinaryExpression extends IBinaryExpression
{
    /** Get type of logical */
    Type getLogicalType();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    /** Type of boolean operation */
    public enum Type
    {
        AND,
        OR
    }
}
