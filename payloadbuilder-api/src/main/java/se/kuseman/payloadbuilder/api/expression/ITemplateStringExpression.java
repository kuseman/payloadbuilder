package se.kuseman.payloadbuilder.api.expression;

import java.util.List;

/** A template string expression. Back tick */
public interface ITemplateStringExpression extends IExpression
{
    /** Get the template expressions */
    List<IExpression> getExpressions();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }
}
