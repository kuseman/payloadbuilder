package se.kuseman.payloadbuilder.api.expression;

/** Logical binary expression */
public interface ILogicalBinaryExpression extends IBinaryExpression
{
    /** Get type of logical */
    Type getLogicalType();

    /** Type of boolean operation */
    public enum Type
    {
        AND,
        OR
    }
}
