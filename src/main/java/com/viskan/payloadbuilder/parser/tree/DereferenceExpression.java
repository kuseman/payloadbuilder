package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public class DereferenceExpression extends Expression
{
    private final Expression left;
    private final Expression right;
    
    public DereferenceExpression(Expression left, Expression right)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }
    
    public Expression getLeft()
    {
        return left;
    }
    
    public Expression getRight()
    {
        return right;
    }

    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return left.isNullable() && left.isNullable();
    }
    
    @Override
    public String toString()
    {
        return left.toString() + "." + right.toString();
    }
}
