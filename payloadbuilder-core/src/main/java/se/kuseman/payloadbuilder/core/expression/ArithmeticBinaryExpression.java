package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;

/** Expression that handles arithmetics +/- etc. */
public class ArithmeticBinaryExpression extends ABinaryExpression implements IArithmeticBinaryExpression
{
    private final IArithmeticBinaryExpression.Type type;

    public ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type type, IExpression left, IExpression right)
    {
        super(left, right);
        this.type = requireNonNull(type, "type");
    }

    @Override
    public IArithmeticBinaryExpression.Type getArithmeticType()
    {
        return type;
    }

    @Override
    public IExpression fold()
    {
        if (left instanceof LiteralNullExpression
                || right instanceof LiteralNullExpression)
        {
            return new LiteralNullExpression(getType());
        }

        boolean lconstant = left.isConstant();
        boolean rconstant = right.isConstant();

        if (lconstant
                && rconstant)
        {
            ValueVector v = eval(TupleVector.CONSTANT, ValueVector.range(0, 1), null);
            if (v.isNull(0))
            {
                return new LiteralNullExpression(getType());
            }

            Column.Type resultType = v.type()
                    .getType();

            switch (resultType)
            {
                case Double:
                    return new LiteralDoubleExpression(v.getDouble(0));
                case Float:
                    return new LiteralFloatExpression(v.getFloat(0));
                case Int:
                    return new LiteralIntegerExpression(v.getInt(0));
                case Long:
                    return new LiteralLongExpression(v.getLong(0));
                case Decimal:
                    return new LiteralDecimalExpression(v.getDecimal(0));
                case String:
                    return new LiteralStringExpression(v.getString(0));
                default:
                    throw new IllegalArgumentException("Unsupported result type: " + resultType);
            }
        }

        return this;
    }

    @Override
    public ResolvedType getType()
    {
        return getType(left.getType(), right.getType());
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return eval(input, ValueVector.range(0, input.getRowCount()), context);
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        ValueVector lvv = left.eval(input, selection, context);
        ValueVector rvv = right.eval(input, selection, context);
        final int rowCount = selection.size();
        final Column.Type resultType = getType(lvv.type(), rvv.type()).getType();
        return new ValueVector()
        {
            @Override
            public boolean isNull(int row)
            {
                return lvv.isNull(row)
                        || rvv.isNull(row);
            }

            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(resultType);
            }

            @Override
            public int size()
            {
                return rowCount;
            }

            @Override
            public int getInt(int row)
            {
                // Implicit cast
                if (resultType != Column.Type.Int)
                {
                    return ValueVector.super.getInt(row);
                }

                // Can only be ints on both sides
                int ileft = lvv.getInt(row);
                int iright = rvv.getInt(row);

                switch (type)
                {
                    case ADD:
                        return Math.addExact(ileft, iright);
                    case DIVIDE:
                        return Math.floorDiv(ileft, iright);
                    case MODULUS:
                        return Math.floorMod(ileft, iright);
                    case MULTIPLY:
                        return Math.multiplyExact(ileft, iright);
                    case SUBTRACT:
                        return Math.subtractExact(ileft, iright);
                    default:
                        throw new IllegalArgumentException("Unsupported operation " + type);
                }
            }

            @Override
            public long getLong(int row)
            {
                // Implicit cast
                if (resultType != Column.Type.Long)
                {
                    return ValueVector.super.getLong(row);
                }

                // Let Vectors cast to correct value
                long lleft = lvv.getLong(row);
                long lright = rvv.getLong(row);

                switch (type)
                {
                    case ADD:
                        return Math.addExact(lleft, lright);
                    case DIVIDE:
                        return Math.floorDiv(lleft, lright);
                    case MODULUS:
                        return Math.floorMod(lleft, lright);
                    case MULTIPLY:
                        return Math.multiplyExact(lleft, lright);
                    case SUBTRACT:
                        return Math.subtractExact(lleft, lright);
                    default:
                        throw new IllegalArgumentException("Unsupported operation " + type);
                }
            }

            @Override
            public Decimal getDecimal(int row)
            {
                // Implicit cast
                if (resultType != Column.Type.Decimal)
                {
                    return ValueVector.super.getDecimal(row);
                }

                Decimal dleft = lvv.getDecimal(row);
                Decimal dright = rvv.getDecimal(row);
                return dleft.processArithmetic(dright, type);
            }

            @Override
            public float getFloat(int row)
            {
                // Let Vectors cast to correct value
                float fleft = lvv.getFloat(row);
                float fright = rvv.getFloat(row);

                switch (type)
                {
                    case ADD:
                        return fleft + fright;
                    case DIVIDE:
                        return fleft / fright;
                    case MODULUS:
                        return fleft % fright;
                    case MULTIPLY:
                        return fleft * fright;
                    case SUBTRACT:
                        return fleft - fright;
                    default:
                        throw new IllegalArgumentException("Unsupported operation " + type);
                }
            }

            @Override
            public double getDouble(int row)
            {
                // Implicit cast
                if (resultType != Column.Type.Double)
                {
                    return ValueVector.super.getDouble(row);
                }

                // Let Vectors cast to correct value
                double dleft = lvv.getDouble(row);
                double dright = rvv.getDouble(row);

                switch (type)
                {
                    case ADD:
                        return dleft + dright;
                    case DIVIDE:
                        return dleft / dright;
                    case MODULUS:
                        return dleft % dright;
                    case MULTIPLY:
                        return dleft * dright;
                    case SUBTRACT:
                        return dleft - dright;
                    default:
                        throw new IllegalArgumentException("Unsuppored operation " + type);
                }
            }

            @Override
            public UTF8String getString(int row)
            {
                // Implicit cast
                if (resultType != Column.Type.String)
                {
                    return ValueVector.super.getString(row);
                }

                // Concats left and right
                return UTF8String.concat(UTF8String.EMPTY, asList(lvv.getString(row), rvv.getString(row)));
            }

            @Override
            public Object getAny(int row)
            {
                if (resultType != Column.Type.Any)
                {
                    throw new IllegalArgumentException("getValue should not be called for typed vectors");
                }

                switch (type)
                {
                    case ADD:
                        return ExpressionMath.add(lvv.getAny(row), rvv.getAny(row));
                    case DIVIDE:
                        return ExpressionMath.divide(lvv.getAny(row), rvv.getAny(row));
                    case MODULUS:
                        return ExpressionMath.modulo(lvv.getAny(row), rvv.getAny(row));
                    case MULTIPLY:
                        return ExpressionMath.multiply(lvv.getAny(row), rvv.getAny(row));
                    case SUBTRACT:
                        return ExpressionMath.subtract(lvv.getAny(row), rvv.getAny(row));
                    default:
                        throw new IllegalArgumentException("Unsuppored operation " + type);

                }
            }
        };
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
        if (obj instanceof ArithmeticBinaryExpression)
        {
            ArithmeticBinaryExpression e = (ArithmeticBinaryExpression) obj;
            return super.equals(obj)
                    && type == e.type;
        }
        return false;
    }

    @Override
    public boolean semanticEquals(se.kuseman.payloadbuilder.api.expression.IExpression expression)
    {
        if (equals(expression))
        {
            return true;
        }
        else if (expression instanceof ArithmeticBinaryExpression)
        {
            ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) expression;

            // Try to switch the arguments if operation is commutative
            if (type == that.type
                    && type.isCommutative()
                    && left.semanticEquals(that.right)
                    && right.semanticEquals(that.left))
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString()
    {
        return left.toString() + " " + type.getSign() + " " + right.toString();
    }

    @Override
    public String toVerboseString()
    {
        return left.toVerboseString() + " " + type.getSign() + " " + right.toVerboseString();
    }

    // CSOFF
    private ResolvedType getType(ResolvedType left, ResolvedType right)
    // CSOFF
    {
        final Column.Type leftType = left.getType();
        final Column.Type rightType = right.getType();

        boolean leftOk = (leftType.isNumber()
                || leftType == Column.Type.String
                || leftType == Column.Type.Any);
        boolean rightOk = (rightType.isNumber()
                || rightType == Column.Type.String
                || rightType == Column.Type.Any);

        if (!leftOk
                && !rightOk)
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftType + " and " + rightType);
        }

        // Left or right side will be promoted

        // Find out correct data type
        Column.Type resultType = leftType;
        if (rightType.getPrecedence() > leftType.getPrecedence())
        {
            resultType = rightType;
        }

        if (resultType.isComplex())
        {
            throw new IllegalArgumentException("Cannot perform arithmetics on types " + leftType + " and " + rightType);
        }
        else if (resultType == Column.Type.String
                && type != Type.ADD)
        {
            throw new IllegalArgumentException("Arithmetics on Strings only supports ADD");
        }

        return ResolvedType.of(resultType);
    }
}
