package se.kuseman.payloadbuilder.api.expression;

/** Definition of a comparison expression */
public interface IComparisonExpression extends IBinaryExpression
{
    /** Return type of comparison */
    Type getComparisonType();

    /** Type */
    public enum Type
    {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_EQUAL,
        GREATER_THAN,
        GREATER_THAN_EQUAL;

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
