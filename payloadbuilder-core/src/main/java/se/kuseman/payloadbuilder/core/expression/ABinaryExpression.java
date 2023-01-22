package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
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

        // If one side has 1 in size then we need to normalize the vectors
        // This happens when we run a nested loop and access outer values.
        // In normal cases we always have the same size because of normalization in joins
        // between vectors

        final int leftSize = lvv.size();
        final int rightSize = rvv.size();

        if (lvv.size() == 1
                && rvv.size() != 1)
        {
            // Normalize left
            lvv = new ValueVectorAdapter(lvv)
            {
                @Override
                public int size()
                {
                    return rightSize;
                }

                @Override
                protected int getRow(int row)
                {
                    return 0;
                }
            };
        }
        else if (lvv.size() != 1
                && rvv.size() == 1)
        {
            // Normalize right
            rvv = new ValueVectorAdapter(rvv)
            {
                @Override
                public int size()
                {
                    return leftSize;
                }

                @Override
                protected int getRow(int row)
                {
                    return 0;
                }
            };
        }

        if (lvv.size() != rvv.size())
        {
            throw new IllegalArgumentException("Evaluation of binary vectors requires equal size");
        }

        return eval(lvv, rvv);
    }

    /** Evaluate left and right value vectors */
    abstract ValueVector eval(ValueVector left, ValueVector right);

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
