package se.kuseman.payloadbuilder.api.expression;

/** Arithmetic binary expression */
public interface IArithmeticBinaryExpression extends IBinaryExpression
{
    /** Get type of arithmetics */
    Type getArithmeticType();

    /** Type */
    public enum Type
    {
        ADD("+", true),
        SUBTRACT("-", false),
        MULTIPLY("*", true),
        DIVIDE("/", false),
        MODULUS("%", false);

        private final String sign;
        private final boolean commutative;

        Type(String sign, boolean commutative)
        {
            this.sign = sign;
            this.commutative = commutative;
        }

        public String getSign()
        {
            return sign;
        }

        public boolean isCommutative()
        {
            return commutative;
        }
    }
}
