package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression.FALSE;
import static se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression.TRUE;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IBooleanVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;

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
    ValueVector eval(IExecutionContext context, int rowCount, ValueVector lvv, ValueVector rvv)
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

        IBooleanVectorBuilder builder = context.getVectorBuilderFactory()
                .getBooleanVectorBuilder(rowCount);

        boolean isAnd = type == Type.AND;

        for (int i = 0; i < rowCount; i++)
        {
            // Optimize for lazy eval, which means we should not touch right side
            // either for isNull or getBoolean if we don't need
            boolean leftNull = lvv.isNull(i);
            boolean lv = !leftNull ? lvv.getBoolean(i)
                    : false;

            if (!leftNull)
            {
                // true AND ? => Continue
                // false AND ? => false
                // true OR ? => true
                // false OR ? => Continue

                // false AND ? => false
                if (!lv
                        && isAnd)
                {
                    builder.put(false);
                    continue;
                }
                // true OR ? => true
                else if (lv
                        && !isAnd)
                {
                    builder.put(true);
                    continue;
                }
            }

            // Here we must start to look at the right side to continue
            boolean rightNull = rvv.isNull(i);

            boolean rv = !rightNull ? rvv.getBoolean(i)
                    : false;

            if (leftNull
                    && rightNull)
            {
                // null AND/OR null => null
                builder.putNull();
                continue;
            }
            else if (rightNull)
            {
                // true AND null
                // false OR null
                builder.putNull();
                continue;
            }
            else if (leftNull)
            {
                // null AND false
                if (!rv
                        && isAnd)
                {
                    builder.put(false);
                    continue;
                }
                // null OR true
                else if (rv
                        && !isAnd)
                {
                    builder.put(true);
                    continue;
                }

                // null AND true
                // null OR false
                builder.putNull();
                continue;
            }

            boolean result = isAnd ? lv
                    && rv
                    : lv
                            || rv;

            builder.put(result);
        }

        return builder.build();

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
