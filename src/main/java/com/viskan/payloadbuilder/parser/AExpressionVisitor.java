package com.viskan.payloadbuilder.parser;

import java.util.List;

/** Visitor adapter for expression visitors */
public abstract class AExpressionVisitor<TR, TC> implements ExpressionVisitor<TR, TC>
{
    /** Default result 
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
    public TR visit(NamedParameterExpression expression, TC context)
    {
        return defaultResult(context);
    }
    
    @Override
    public TR visit(VariableExpression expression, TC context)
    {
        return defaultResult(context);
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
    public TR visit(QualifiedReferenceExpression expression, TC context)
    {
        return defaultResult(context);
    }

    @Override
    public TR visit(NestedExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
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
    public TR visit(LambdaExpression expression, TC context)
    {
        return visitChildren(context, expression.getExpression());
    }
    
    private TR visitChildren(TC context, Expression ... args)
    {
        return visitChildren(context, null, args);
    }
    
    protected TR visitChildren(TC context, List<Expression> expressions, Expression ... args)
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
