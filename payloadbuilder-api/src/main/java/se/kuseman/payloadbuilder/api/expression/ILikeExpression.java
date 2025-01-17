package se.kuseman.payloadbuilder.api.expression;

/** Definition of LIKE expression */
public interface ILikeExpression extends IExpression
{
    /** Return true if this is expression in a NOT IN */
    boolean isNot();

    /** Return left side expression */
    IExpression getExpression();

    /** Return pattern expression for this LIKE expression */
    IExpression getPatternExpression();

    /** Return the escape character expression for this LIKE expression */
    IExpression getEscapeCharacterExpression();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
