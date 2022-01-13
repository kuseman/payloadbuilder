package org.kuse.payloadbuilder.core.parser;

import java.util.ArrayList;
import java.util.List;

import org.kuse.payloadbuilder.core.parser.CaseExpression.WhenClause;

/** Visitor adapter for expression visitors */
//CSOFF
public abstract class AExpressionVisitor<TR, TC> implements ExpressionVisitor<TR, TC>
//CSON
{
    /**
     * Default result
     *
     * @param context Context for visitor
     **/
    protected TR defaultResult(TC context)
    {
        return null;
    }

    /** Aggregate results */
    @SuppressWarnings("unused")
    protected TR aggregate(TR result, TR nextResult)
    {
        return nextResult;
    }

    @Override
    public TR visit(LiteralNullExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(LiteralBooleanExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(LiteralIntegerExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(LiteralLongExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(LiteralFloatExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(LiteralDoubleExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(LiteralStringExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(VariableExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(CaseExpression expression, TC context)
    {
        List<Expression> children = new ArrayList<>(expression.getWhenClauses().size() * 2 + 1);
        for (WhenClause whenClause : expression.getWhenClauses())
        {
            children.add(whenClause.getCondition());
            children.add(whenClause.getResult());
        }
        if (expression.getElseExpression() != null)
        {
            children.add(expression.getElseExpression());
        }
        return visitChildren(context, children);
    }

    @Override
    public TR visit(ComparisonExpression expression, TC context)
    {
        return visitChildren(context, expression.getLeft(), expression.getRight());
    }

    @Override
    public TR visit(ArithmeticUnaryExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
    }

    @Override
    public TR visit(ArithmeticBinaryExpression expression, TC context)
    {
        return visitChildren(context, expression.getLeft(), expression.getRight());
    }

    @Override
    public TR visit(LogicalBinaryExpression expression, TC context)
    {
        return visitChildren(context, expression.getLeft(), expression.getRight());
    }

    @Override
    public TR visit(LogicalNotExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
    }

    @Override
    public TR visit(InExpression expression, TC context)
    {
        return visitChildren(context, expression.getArguments(), expression.getExpression());
    }

    @Override
    public TR visit(LikeExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression(), expression.getPatternExpression());
    }

    @Override
    public TR visit(QualifiedReferenceExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(UnresolvedQualifiedReferenceExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(NestedExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
    }

    @Override
    public TR visit(UnresolvedSubQueryExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(NullPredicateExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
    }

    @Override
    public TR visit(QualifiedFunctionCallExpression expression, TC context)
    {
        return visitChildren(context, expression.getArguments(), (Expression[]) null);
    }

    @Override
    public TR visit(DereferenceExpression expression, TC context)
    {
        return visitChildren(context, expression.getLeft(), expression.getRight());
    }

    @Override
    public TR visit(UnresolvedDereferenceExpression expression, TC context)
    {
        return visitChildren(context, expression.getLeft(), expression.getRight());
    }

    @Override
    public TR visit(SubscriptExpression expression, TC context)
    {
        return visitChildren(context, expression.getValue(), expression.getSubscript());
    }

    @Override
    public TR visit(LambdaExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
    }

    private TR visitChildren(TC context, Expression... args)
    {
        return visitChildren(context, null, args);
    }

    /** Visit provided expressions */
    protected TR visitChildren(TC context, List<Expression> expressions, Expression... args)
    {
        TR result = defaultResult(context);
        if (expressions != null)
        {
            for (Expression e : expressions)
            {
                TR value = e.accept(this, context);
                result = aggregate(result, value);
            }
        }

        if (args != null)
        {
            for (Expression e : args)
            {
                TR value = e.accept(this, context);
                result = aggregate(result, value);
            }
        }
        return result;
    }
}
