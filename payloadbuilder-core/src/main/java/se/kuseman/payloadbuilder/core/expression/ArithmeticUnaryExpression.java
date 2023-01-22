package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IArithmeticUnaryExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

/** Arithmetic unary expression */
public class ArithmeticUnaryExpression implements IArithmeticUnaryExpression
{
    private final IArithmeticUnaryExpression.Type type;
    private final IExpression expression;

    public ArithmeticUnaryExpression(IArithmeticUnaryExpression.Type type, IExpression expression)
    {
        this.type = requireNonNull(type, "type");
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public Type getArithmeticType()
    {
        return type;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public ResolvedType getType()
    {
        // The type stays the same when switching signum
        return expression.getType();
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);

        if (!(value.type()
                .getType()
                .isNumber()
                || value.type()
                        .getType() == Column.Type.Any))
        {
            throw new IllegalArgumentException("Cannot negate '" + value.type() + "'");
        }

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return value.type();
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public int getInt(int row)
            {
                return -value.getInt(row);
            }

            @Override
            public long getLong(int row)
            {
                return -value.getLong(row);
            }

            @Override
            public float getFloat(int row)
            {
                return -value.getFloat(row);
            }

            @Override
            public double getDouble(int row)
            {
                return -value.getDouble(row);
            }

            @Override
            public Object getValue(int row)
            {
                return ExpressionMath.negate(value.getValue(row));
            }
        };
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof ArithmeticUnaryExpression)
        {
            ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) obj;
            return expression.equals(that.expression)
                    && type == that.type;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return type.getSign() + expression.toString();
    }

    @Override
    public String toVerboseString()
    {
        return type.getSign() + expression.toVerboseString();
    }

    /** Operation */
    public enum Operation
    {
        PLUS("+"),
        MINUS("-");

        private final String value;

        Operation(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
    }
}
