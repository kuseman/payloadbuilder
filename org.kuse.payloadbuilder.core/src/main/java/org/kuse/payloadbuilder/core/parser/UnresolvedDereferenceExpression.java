package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Unresolved Dereference expression */
public class UnresolvedDereferenceExpression extends Expression implements HasIdentifier
{
    private final Expression left;
    private final UnresolvedQualifiedReferenceExpression right;

    public UnresolvedDereferenceExpression(Expression left, UnresolvedQualifiedReferenceExpression right)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }

    public Expression getLeft()
    {
        return left;
    }

    public UnresolvedQualifiedReferenceExpression getRight()
    {
        return right;
    }

    @Override
    public String identifier()
    {
        return right.identifier();
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        throw new IllegalStateException("Cannot evaluate " + getClass().getName());
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        throw new IllegalStateException("Cannot generate code for " + getClass().getName());
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + left.hashCode();
        hashCode = hashCode * 37 + right.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof UnresolvedDereferenceExpression)
        {
            UnresolvedDereferenceExpression that = (UnresolvedDereferenceExpression) obj;
            return left.equals(that.left)
                && right.equals(that.right);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return left.toString() + "." + right.toString();
    }
}
