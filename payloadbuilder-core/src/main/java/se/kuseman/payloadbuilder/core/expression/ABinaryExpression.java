package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Base class for binary expressions. Handling normalization of vectors etc. before evaluation */
abstract class ABinaryExpression implements IExpression
{
    protected final IExpression left;
    protected final IExpression right;

    ABinaryExpression(IExpression left, IExpression right)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }

    public IExpression getLeft()
    {
        return left;
    }

    public IExpression getRight()
    {
        return right;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(left, right);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ValueVector lvv = left.eval(input, context);
        ValueVector rvv = right.eval(input, context);
        return eval(context, input.getRowCount(), lvv, rvv);
    }

    /** Evaluate left and right value vectors */
    abstract ValueVector eval(IExecutionContext context, int rowCount, ValueVector left, ValueVector right);

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + left.hashCode();
        hashCode = hashCode * 37 + right.hashCode();
        return hashCode;
        // CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof ABinaryExpression)
        {
            ABinaryExpression that = (ABinaryExpression) obj;
            return left.equals(that.left)
                    && right.equals(that.right);
        }

        return false;
    }

}
