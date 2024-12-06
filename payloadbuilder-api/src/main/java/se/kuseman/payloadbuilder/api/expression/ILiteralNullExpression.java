package se.kuseman.payloadbuilder.api.expression;

/** Literal stringILiteralDoubleExpression.java */
public interface ILiteralNullExpression extends ILiteralExpression
{
    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
