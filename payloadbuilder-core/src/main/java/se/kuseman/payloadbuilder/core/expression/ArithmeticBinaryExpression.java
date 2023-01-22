package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

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

    // @Override
    // public IExpression fold()
    // {
    // if (left instanceof LiteralNullExpression
    // || right instanceof LiteralNullExpression)
    // {
    // return new LiteralNullExpression(Type.Object);
    // }
    //
    // boolean ll = left instanceof LiteralExpression;
    // boolean rl = right instanceof LiteralExpression;
    //
    // if (ll
    // || rl)
    // {
    // if (ll
    // && rl)
    // {
    // return LiteralExpression.create(evalInternal(((LiteralExpression) left).getObjectValue(), ((LiteralExpression) right).getObjectValue()));
    // }
    //
    // // TODO: more folding. multiply 0, 1
    // // divide 1
    // }
    //
    // return this;
    // }

    @Override
    public ResolvedType getType()
    {
        return getType(left.getType(), right.getType());
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    ValueVector eval(ValueVector lvv, ValueVector rvv)
    {
        final Column.Type resultType = getType(lvv.type(), rvv.type()).getType();

        // We use a lazy vector here to avoid allocation of a lot of primitive arrays
        final boolean nullable = lvv.isNullable()
                || rvv.isNullable();
        final int size = lvv.size();
        return new ValueVector()
        {
            @Override
            public boolean isNullable()
            {
                return nullable;
            }

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
                return size;
            }

            @Override
            public int getInt(int row)
            {
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
            public Object getValue(int row)
            {
                if (resultType != Column.Type.Any)
                {
                    throw new IllegalArgumentException("getValue should not be called for typed vectors");
                }

                switch (type)
                {
                    case ADD:
                        return ExpressionMath.add(lvv.getValue(row), rvv.getValue(row));
                    case DIVIDE:
                        return ExpressionMath.divide(lvv.getValue(row), rvv.getValue(row));
                    case MODULUS:
                        return ExpressionMath.modulo(lvv.getValue(row), rvv.getValue(row));
                    case MULTIPLY:
                        return ExpressionMath.multiply(lvv.getValue(row), rvv.getValue(row));
                    case SUBTRACT:
                        return ExpressionMath.subtract(lvv.getValue(row), rvv.getValue(row));
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
                || leftType == Column.Type.Any);
        boolean rightOk = (rightType.isNumber()
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
        return ResolvedType.of(resultType);
    }
}
