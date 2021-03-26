package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Tuple;

/** Dereference expression */
public class DereferenceExpression extends Expression implements HasIdentifier
{
    private final Expression left;
    private final QualifiedReferenceExpression right;

    public DereferenceExpression(Expression left, QualifiedReferenceExpression right)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }

    public Expression getLeft()
    {
        return left;
    }

    public QualifiedReferenceExpression getRight()
    {
        return right;
    }

    @Override
    public String identifier()
    {
        return right.getQname().getLast();
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object leftResult = left.eval(context);

        if (leftResult == null)
        {
            return null;
        }

        if (leftResult instanceof Tuple)
        {
            context.setTuple((Tuple) leftResult);
            return right.eval(context);
        }
        else if (leftResult instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) leftResult;
            // A dereference only has one part so pick first and use as key
            return map.get(right.getQname().getFirst());
        }

        throw new IllegalArgumentException("Cannot dereference " + leftResult.getClass().getSimpleName() + " value: " + leftResult);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        return left.isNullable() && left.isNullable();
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
        if (obj instanceof DereferenceExpression)
        {
            DereferenceExpression that = (DereferenceExpression) obj;
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
