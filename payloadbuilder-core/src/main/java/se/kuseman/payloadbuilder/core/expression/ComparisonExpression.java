package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.BitSet;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;
import se.kuseman.payloadbuilder.core.physicalplan.BitSetVector;

/** Comparison expression */
public class ComparisonExpression extends ABinaryExpression implements IComparisonExpression
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
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    ValueVector eval(ValueVector lvv, ValueVector rvv)
    {
        // Determine which type to use for vectors
        Column.Type leftType = lvv.type()
                .getType();
        Column.Type rightType = rvv.type()
                .getType();
        Column.Type resultType = leftType;
        if (rightType.getPrecedence() > resultType.getPrecedence())
        {
            resultType = rightType;
        }

        int size = lvv.size();
        BitSet bs = new BitSet(size);
        BitSet nullBs = null;

        for (int i = 0; i < size; i++)
        {
            boolean leftNull = lvv.isNullable()
                    && lvv.isNull(i);
            boolean rightNull = rvv.isNullable()
                    && rvv.isNull(i);

            if (leftNull
                    || rightNull)
            {
                if (nullBs == null)
                {
                    nullBs = new BitSet(size);
                }
                nullBs.set(i, true);
            }
            else
            {
                bs.set(i, compare(i, resultType, lvv, rvv));
            }
        }

        return new BitSetVector(size, bs, nullBs);
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

    private boolean compare(int row, Column.Type vectorType, ValueVector left, ValueVector right)
    {
        int c;
        switch (vectorType)
        {
            case Boolean:
                c = Boolean.compare(left.getBoolean(row), right.getBoolean(row));
                break;
            case Double:
                c = Double.compare(left.getDouble(row), right.getDouble(row));
                break;
            case Float:
                c = Float.compare(left.getFloat(row), right.getFloat(row));
                break;
            case Int:
                c = Integer.compare(left.getInt(row), right.getInt(row));
                break;
            case Long:
                c = Long.compare(left.getLong(row), right.getLong(row));
                break;
            case String:
                UTF8String refL = left.getString(row);
                UTF8String refR = right.getString(row);
                c = refL.compareTo(refR);
                break;
            default:
                // Reflective compare
                c = ExpressionMath.cmp(left.getValue(row), right.getValue(row));
                break;
        }

        switch (type)
        {
            case EQUAL:
                return c == 0;
            case NOT_EQUAL:
                return c != 0;
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
}
