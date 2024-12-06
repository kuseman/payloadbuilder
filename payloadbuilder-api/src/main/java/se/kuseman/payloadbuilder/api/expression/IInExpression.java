package se.kuseman.payloadbuilder.api.expression;

import java.util.List;

/** Definition of a an IN expression */
public interface IInExpression extends IExpression
{
    /** Return true if this is expression in a NOT IN */
    boolean isNot();

    /** Return the left side expression */
    IExpression getExpression();

    /** Return IN arguments */
    List<IExpression> getArguments();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
