package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Comparison expression */
public class ComparisonExpression extends ABinaryExpression implements IComparisonExpression, Invertable
{
    private final IComparisonExpression.Type type;

    public ComparisonExpression(IComparisonExpression.Type type, IExpression left, IExpression right)
    {
        super(left, right);
        this.type = requireNonNull(type, "type");
    }

    @Override
    public IComparisonExpression.Type getComparisonType()
    {
        return type;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Column.Type.Boolean);
    }

    @Override
    public IExpression getInvertedExpression()
    {
        return new ComparisonExpression(type.getInvertedType(), left, right);
    }

    @Override
    public IExpression fold()
    {
        boolean lconstant = left.isConstant();
        boolean rconstant = right.isConstant();

        if (lconstant
                && rconstant)
        {
            ValueVector l = left.eval(null);
            ValueVector r = right.eval(null);
            Column.Type resultType = getVectorComparisonType(l, r);

            if (l.isNull(0)
                    || r.isNull(0))
            {
                return new LiteralNullExpression(ResolvedType.of(Column.Type.Boolean));
            }

            return compare(l, r, resultType, 0, 0) ? LiteralBooleanExpression.TRUE
                    : LiteralBooleanExpression.FALSE;
        }

        return this;
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        int size = selection.size();
        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Column.Type.Boolean), size);

        // First evaluate left side and take care of all nulls, these should not be evaluated on the right side
        ValueVector leftVector = left.eval(input, selection, context);
        int[] rightRows = null;
        int[] leftRows = null;

        int index = 0;
        for (int i = 0; i < size; i++)
        {
            if (leftVector.isNull(i))
            {
                resultVector.setNull(i);
            }
            else
            {
                if (rightRows == null)
                {
                    rightRows = new int[size];
                    leftRows = new int[size];
                }
                rightRows[index] = selection.getInt(i);
                leftRows[index] = i;
                index++;
            }
        }

        // All nulls, no need to evaluate right
        if (rightRows == null)
        {
            return resultVector;
        }

        ValueVector rightSelection = VectorUtils.convertToSelectionVector(rightRows, index);
        ValueVector rightVector = right.eval(input, rightSelection, context);

        size = rightSelection.size();
        Column.Type resultType = getVectorComparisonType(leftVector, rightVector);
        for (int i = 0; i < size; i++)
        {
            int row = leftRows[i];
            if (rightVector.isNull(i))
            {
                resultVector.setNull(row);
            }
            else
            {
                boolean result = compare(leftVector, rightVector, resultType, row, i);
                resultVector.setBoolean(row, result);
            }
        }

        return resultVector;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return eval(input, ValueVector.range(0, input.getRowCount()), context);
    }

    private Column.Type getVectorComparisonType(ValueVector lvv, ValueVector rvv)
    {
        // Determine which type to use for vectors
        Column.Type leftType = lvv.type()
                .getType();
        Column.Type rightType = rvv.type()
                .getType();
        return rightType.getPrecedence() > leftType.getPrecedence() ? rightType
                : leftType;
    }

    private boolean compare(ValueVector left, ValueVector right, Column.Type resultType, int leftRow, int rightRow)
    {
        if (type == Type.EQUAL)
        {
            return VectorUtils.equals(left, right, resultType, leftRow, rightRow, false);
        }
        else if (type == Type.NOT_EQUAL)
        {
            return !VectorUtils.equals(left, right, resultType, leftRow, rightRow, false);
        }

        int c = VectorUtils.compare(left, right, resultType, leftRow, rightRow);
        switch (type)
        {
            case GREATER_THAN:
                return c > 0;
            case GREATER_THAN_EQUAL:
                return c >= 0;
            case LESS_THAN:
                return c < 0;
            case LESS_THAN_EQUAL:
                return c <= 0;
            default:
                throw new IllegalArgumentException("Unsupported comparison op " + type);
        }
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

        if (obj instanceof ComparisonExpression)
        {
            ComparisonExpression that = (ComparisonExpression) obj;
            return super.equals(obj)
                    && type == that.type;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", left, type, right);
    }

    @Override
    public String toVerboseString()
    {
        return String.format("%s %s %s", left.toVerboseString(), type, right.toVerboseString());
    }
}
