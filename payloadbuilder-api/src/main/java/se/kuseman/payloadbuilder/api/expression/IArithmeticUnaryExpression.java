package se.kuseman.payloadbuilder.api.expression;

/** Arithmetic unary expression */
public interface IArithmeticUnaryExpression extends IUnaryExpression
{
    /** Get type of expression */
    Type getArithmeticType();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    /** Type */
    public enum Type
    {
        PLUS("+"),
        MINUS("-");

        private final String sign;

        Type(String sign)
        {
            this.sign = sign;
        }

        public String getSign()
        {
            return sign;
        }
    }
}
