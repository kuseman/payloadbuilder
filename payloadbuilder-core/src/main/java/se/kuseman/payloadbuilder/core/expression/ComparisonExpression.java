package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IBooleanVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
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
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
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
            Column.Type resultType = getResultType(l, r);

            if (l.isNull(0)
                    || r.isNull(0))
            {
                return new LiteralNullExpression(ResolvedType.of(Column.Type.Boolean));
            }

            return compare(l, r, resultType, 0) ? LiteralBooleanExpression.TRUE
                    : LiteralBooleanExpression.FALSE;
        }

        return this;
    }

    @Override
    ValueVector eval(IExecutionContext context, int rowCount, ValueVector lvv, ValueVector rvv)
    {
        Column.Type resultType = getResultType(lvv, rvv);
        IBooleanVectorBuilder builder = context.getVectorBuilderFactory()
                .getBooleanVectorBuilder(rowCount);
        for (int row = 0; row < rowCount; row++)
        {
            boolean isNull = lvv.isNull(row)
                    || rvv.isNull(row);
            if (isNull)
            {
                builder.putNull();
            }
            else
            {
                boolean result = compare(lvv, rvv, resultType, row);
                builder.put(result);
            }
        }

        return builder.build();
    }

    private Column.Type getResultType(ValueVector lvv, ValueVector rvv)
    {
        // Determine which type to use for vectors
        Column.Type leftType = lvv.type()
                .getType();
        Column.Type rightType = rvv.type()
                .getType();
        return rightType.getPrecedence() > leftType.getPrecedence() ? rightType
                : leftType;
    }

    private boolean compare(ValueVector left, ValueVector right, Column.Type resultType, int row)
    {
        if (type == Type.EQUAL)
        {
            return VectorUtils.equals(left, right, resultType, row, row, false);
        }
        else if (type == Type.NOT_EQUAL)
        {
            return !VectorUtils.equals(left, right, resultType, row, row, false);
        }

        int c = VectorUtils.compare(left, right, resultType, row, row);
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
