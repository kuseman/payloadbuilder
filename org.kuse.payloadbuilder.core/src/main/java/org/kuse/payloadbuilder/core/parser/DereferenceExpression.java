package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import java.util.Map;

import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.Row.ChildRows;

public class DereferenceExpression extends Expression
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
    public Object eval(ExecutionContext context)
    {
        Object leftResult = left.eval(context);

        if (leftResult == null)
        {
            return null;
        }
        
        if (leftResult instanceof Row)
        {
            context.setRow((Row) leftResult);
            return right.eval(context);
        }
        else if (leftResult instanceof Map)
        {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) leftResult;
            // A dereference only has one part so pick first and use as key
            return map.get(right.getQname().getFirst());
        }
        // Child rows de-reference, extract first row
        else if (leftResult instanceof ChildRows && ((ChildRows) leftResult).size() > 0)
        {
            context.setRow(((ChildRows) leftResult).get(0));
            return right.eval(context);
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
        return 17
            + (37 * left.hashCode())
            + (37 * right.hashCode());
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
