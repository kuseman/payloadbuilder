package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.ICastExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

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
            throw new IllegalArgumentException("Cannot cast to complex types");
        }
        else if (type.getType() == Type.ValueVector
                && type.getSubType()
                        .getType() != Type.Any)
        {
            throw new IllegalArgumentException("Cannot only cast to Any arrays types");
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
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);

        // Same type no cast is needed
        if (value.type()
                .equals(type))
        {
            return value;
        }

        // Use implicit casts in ValueVector
        return new ValueVectorAdapter(value)
        {
            @Override
            public ResolvedType type()
            {
                return type;
            }

            @Override
            public Object getValue(int row)
            {
                // Convert value to ValueVector
                if (type.getType() == Type.ValueVector)
                {
                    Object obj = value.valueAsObject(row);
                    if (obj instanceof ValueVector)
                    {
                        return obj;
                    }
                    return VectorUtils.convertToValueVector(obj);
                }
                return super.getValue(row);
            }
        };
    }

    @Override
    public ResolvedType getType()
    {
        return type;
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
