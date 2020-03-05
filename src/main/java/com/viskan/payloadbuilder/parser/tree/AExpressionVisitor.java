package com.viskan.payloadbuilder.parser.tree;

/** Visitor adapter for expression visitors */
public abstract class AExpressionVisitor<TR, TC> implements ExpressionVisitor<TR, TC>
{
    @Override
    public TR visit(LiteralNullExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(LiteralBooleanExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(LiteralNumericExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(LiteralDecimalExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(LiteralStringExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(ComparisonExpression expression, TC context)
    {
        expression.getLeft().accept(this, context);
        expression.getRight().accept(this, context);
        return null;
    }

    @Override
    public TR visit(ArithmeticUnaryExpression expression, TC context)
    {
        expression.getExpression().accept(this, context);
        return null;
    }

    @Override
    public TR visit(ArithmeticBinaryExpression expression, TC context)
    {
        expression.getLeft().accept(this, context);
        expression.getRight().accept(this, context);
        return null;
    }

    @Override
    public TR visit(LogicalBinaryExpression expression, TC context)
    {
        expression.getLeft().accept(this, context);
        expression.getRight().accept(this, context);
        return null;
    }

    @Override
    public TR visit(LogicalNotExpression expression, TC context)
    {
        expression.getExpression().accept(this, context);
        return null;
    }

    @Override
    public TR visit(InExpression expression, TC context)
    {
        expression.getExpression().accept(this, context);
        expression.getArguments().forEach(arg -> arg.accept(this, context));
        return null;
    }

    @Override
    public TR visit(QualifiedReferenceExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(NestedExpression expression, TC context)
    {
        expression.getExpression().accept(this, context);
        return null;
    }

    @Override
    public TR visit(NullPredicateExpression expression, TC context)
    {
        return null;
    }

    @Override
    public TR visit(QualifiedFunctionCallExpression expression, TC context)
    {
        expression.getArguments().forEach(arg -> arg.accept(this, context));
        return null;
    }

    @Override
    public TR visit(DereferenceExpression expression, TC context)
    {
        expression.getLeft().accept(this, context);
        expression.getRight().accept(this, context);
        return null;
    }

    @Override
    public TR visit(LambdaExpression expression, TC context)
    {
        expression.getExpression().accept(this, context);
        return null;
    }
}
