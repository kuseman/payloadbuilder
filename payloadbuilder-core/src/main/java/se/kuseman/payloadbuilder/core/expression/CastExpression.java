package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.ICastExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Explicit cast operator/expression */
public class CastExpression implements ICastExpression
{
    private final IExpression expression;
    /** Target type */
    private final ResolvedType type;

    public CastExpression(IExpression expression, ResolvedType type)
    {
        this.expression = requireNonNull(expression, "expression");
        this.type = requireNonNull(type, "type");

        if (type.getType()
                .isComplex())
        {
            if (type.getType() != Type.Array)
            {
                throw new IllegalArgumentException("Cannot cast to complex types");
            }
            if (type.getType() == Type.Array
                    && type.getSubType()
                            .getType() != Type.Any)
            {
                throw new IllegalArgumentException("Cannot only cast to Any arrays types");
            }
        }
        else if (type.getType() == Type.Any)
        {
            throw new IllegalArgumentException("Cannot cast to " + Type.Any);
        }
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return singletonList(expression);
    }

    @Override
    public IExpression fold()
    {
        if (expression instanceof LiteralExpression le)
        {
            ValueVector eval = le.eval(null);

            if (eval.isNull(0))
            {
                return new LiteralNullExpression(type);
            }

            // CSOFF
            switch (type.getType())
            // CSON
            {
                case Array:
                    // Convert value to ValueVector
                    Object obj = eval.valueAsObject(0);
                    if (obj instanceof ValueVector vec)
                    {
                        return new LiteralArrayExpression(vec);
                    }
                    return new LiteralArrayExpression(VectorUtils.convertToValueVector(obj));
                case Boolean:
                    return eval.getBoolean(0) ? LiteralBooleanExpression.TRUE
                            : LiteralBooleanExpression.FALSE;
                case DateTime:
                    return new LiteralDateTimeExpression(eval.getDateTime(0));
                case DateTimeOffset:
                    return new LiteralDateTimeOffsetExpression(eval.getDateTimeOffset(0));
                case Decimal:
                    return new LiteralDecimalExpression(eval.getDecimal(0));
                case Double:
                    return new LiteralDoubleExpression(eval.getDouble(0));
                case Float:
                    return new LiteralFloatExpression(eval.getFloat(0));
                case Int:
                    return new LiteralIntegerExpression(eval.getInt(0));
                case Long:
                    return new LiteralLongExpression(eval.getLong(0));
                case String:
                    return new LiteralStringExpression(eval.getString(0));
                case Object:
                case Table:
                case Any:
                    throw new IllegalArgumentException("Cannot cast to " + eval.type()
                            .getType());
                // NOTE!! No default case here
            }
        }
        return this;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);

        // Same type no cast is needed
        if (value.type()
                .equals(type))
        {
            return value;
        }

        if (!type.getType()
                .isComplex())
        {
            MutableValueVector resultVector = context.getVectorFactory()
                    .getMutableVector(type, input.getRowCount());
            resultVector.copy(0, value);
            return resultVector;
        }

        return new ValueVectorAdapter(value)
        {
            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public ResolvedType type()
            {
                return type;
            }

            @Override
            public ValueVector getArray(int row)
            {
                // Implicit cast
                if (type.getType() != Type.Array)
                {
                    return super.getArray(row);
                }

                // Convert value to ValueVector
                Object obj = value.valueAsObject(row);
                if (obj instanceof ValueVector)
                {
                    return (ValueVector) obj;
                }
                return VectorUtils.convertToValueVector(obj);
            }
        };
    }

    @Override
    public ResolvedType getType()
    {
        return type;
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
        else if (obj instanceof CastExpression)
        {
            CastExpression that = (CastExpression) obj;
            return expression.equals(that.expression)
                    && type.equals(that.type);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "CAST(" + expression + " AS " + type.toTypeString() + ")";
    }

    @Override
    public String toVerboseString()
    {
        return "CAST(" + expression.toVerboseString() + " AS " + type.toTypeString() + ")";
    }
}
