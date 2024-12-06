package se.kuseman.payloadbuilder.api.expression;

/** Definition of a comparison expression */
public interface IComparisonExpression extends IBinaryExpression
{
    /** Return type of comparison */
    Type getComparisonType();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    /** Type */
    public enum Type
    {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_EQUAL,
        GREATER_THAN,
        GREATER_THAN_EQUAL;

        /** Return the inverted comparison type */
        public Type getInvertedType()
        {
            switch (this)
            {
                case EQUAL:
                    // !(a = 10) => a != 10
                    return NOT_EQUAL;
                case GREATER_THAN:
                    // !(a > 10) => a <= 10
                    return LESS_THAN_EQUAL;
                case GREATER_THAN_EQUAL:
                    // !(a >= 10) => a < 10
                    return LESS_THAN;
                case LESS_THAN:
                    // !(a < 10) => a >= 10
                    return GREATER_THAN_EQUAL;
                case LESS_THAN_EQUAL:
                    // !(a <= 10) => a > 10
                    return GREATER_THAN;
                case NOT_EQUAL:
                    // !(a != 10) => a = 10
                    return EQUAL;
                default:
                    throw new IllegalArgumentException("Unknown type " + this);
            }
        }

        @Override
        public String toString()
        {
            switch (this)
            {
                case EQUAL:
                    return "=";
                case GREATER_THAN:
                    return ">";
                case GREATER_THAN_EQUAL:
                    return ">=";
                case LESS_THAN:
                    return "<";
                case LESS_THAN_EQUAL:
                    return "<=";
                case NOT_EQUAL:
                    return "!=";
                default:
                    throw new IllegalArgumentException("Unknown type " + this);
            }
        }
    }
}
