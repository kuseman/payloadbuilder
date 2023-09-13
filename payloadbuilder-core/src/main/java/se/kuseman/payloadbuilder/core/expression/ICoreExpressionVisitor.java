package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** Visitor used in core module with expressions only used during parsing/planning */
public interface ICoreExpressionVisitor<T, C> extends IExpressionVisitor<T, C>
{
    default T visit(AsteriskExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(UnresolvedColumnExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(UnresolvedFunctionCallExpression expression, C context)
    {
        return visitChildren(context, expression);
    };

    default T visit(UnresolvedSubQueryExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(LambdaExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(AssignmentExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(AggregateWrapperExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

    default T visit(AliasExpression expression, C context)
    {
        return visitChildren(context, expression);
    }

}
