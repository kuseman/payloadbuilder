package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression.FALSE;
import static se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression.TRUE;

import java.util.BitSet;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.physicalplan.BitSetVector;

/** AND/OR expression */
public class LogicalBinaryExpression extends ABinaryExpression implements ILogicalBinaryExpression
{
    private final Type type;

    public LogicalBinaryExpression(Type type, IExpression left, IExpression right)
    {
        super(left, right);
        this.type = requireNonNull(type, "type");
    }

    @Override
    public Type getLogicalType()
    {
        return type;
    }

    @Override
    public IExpression fold()
    {
        // https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
        // AND - false if either side is false or null
        if (type == Type.AND)
        {
            if (isFalse(left)
                    || isFalse(right))
            {
                return FALSE;
            }
            else if (isTrue(left))
            {
                return right;
            }
            else if (isTrue(right))
            {
                return left;
            }
        }
        // OR 3vl True if either side is true or null */
        else if (isFalse(left))
        {
            return right;
        }
        else if (isFalse(right))
        {
            return left;
        }
        else if (isTrue(left)
                || isTrue(right))
        {
            return TRUE;
        }

        return this;
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
        Column.Type leftType = lvv.type()
                .getType();
        Column.Type rightType = rvv.type()
                .getType();

        if (!(leftType == Column.Type.Boolean
                || leftType == Column.Type.Any)
                || !(rightType == Column.Type.Boolean
                        || rightType == Column.Type.Any))
        {
            throw new IllegalArgumentException("Performing logical binary operation between two value vectors requires boolean type.");
        }

        // Optimized versions
        if (lvv instanceof BitSetVector)
        {
            if (type == Type.AND)
            {
                return ((BitSetVector) lvv).and(rvv);
            }
            return ((BitSetVector) lvv).or(rvv);
        }
        else if (rvv instanceof BitSetVector)
        {
            if (type == Type.AND)
            {
                return ((BitSetVector) rvv).and(lvv);
            }
            return ((BitSetVector) rvv).or(lvv);
        }

        int size = lvv.size();
        BitSet bs = new BitSet(size);
        BitSet nullBs = new BitSet(size);
        boolean isAnd = type == Type.AND;

        for (int i = 0; i < size; i++)
        {
            boolean leftNull = lvv.isNullable()
                    && lvv.isNull(i);
            boolean rightNull = rvv.isNullable()
                    && rvv.isNull(i);

            if (leftNull
                    && rightNull)
            {
                // null and/or null => null
                nullBs.set(i, true);
                continue;
            }
            else if (leftNull)
            {
                // null AND true => null
                // null AND false => false
                // null OR true => true
                // null OR false => null

                boolean rv = rvv.getBoolean(i);
                if ((isAnd
                        && rv)
                        || (!isAnd
                                && !rv))
                {
                    nullBs.set(i, true);
                }
                else
                {
                    bs.set(i, rv);
                }
            }
            else if (rightNull)
            {
                // true AND null => null
                // false AND null => false
                // true OR null => true
                // false OR null => null

                boolean lv = lvv.getBoolean(i);
                if ((lv
                        && isAnd)
                        || (!lv
                                && !isAnd))
                {
                    nullBs.set(i, true);
                }
                else
                {
                    bs.set(i, lv);
                }
            }
            else
            {
                boolean lv = lvv.getBoolean(i);

                if (isAnd
                        && !lv)
                {
                    // false AND <any> => false
                    continue;
                }
                else if (!isAnd
                        && lv)
                {
                    // true or <any> => true
                    bs.set(i, true);
                    continue;
                }

                boolean rv = rvv.getBoolean(i);
                if (isAnd)
                {
                    bs.set(i, lv
                            && rv);
                }
                else
                {
                    bs.set(i, lv
                            || rv);
                }
            }
        }

        return new BitSetVector(size, bs, nullBs);
    }

    private boolean isTrue(IExpression expression)
    {
        return TRUE.equals(expression);
    }

    private boolean isFalse(IExpression expression)
    {
        return FALSE.equals(expression);
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
        if (obj instanceof LogicalBinaryExpression)
        {
            LogicalBinaryExpression that = (LogicalBinaryExpression) obj;
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
