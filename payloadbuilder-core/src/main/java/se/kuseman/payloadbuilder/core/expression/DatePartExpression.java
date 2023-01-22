package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.EpochDateTime;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** Date part function expression */
public class DatePartExpression implements IDatePartExpression
{
    private final Part part;
    private final IExpression expression;

    public DatePartExpression(Part part, IExpression expression)
    {
        this.expression = requireNonNull(expression, "expression");
        this.part = requireNonNull(part, "part");
    }

    @Override
    public Part getPart()
    {
        return part;
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
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
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
                EpochDateTime dateTime = value.getDateTime(row);
                return dateTime.toZonedDateTime()
                        .get(part.getChronoField());
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Int);
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
        else if (obj instanceof DatePartExpression)
        {
            DatePartExpression that = (DatePartExpression) obj;
            return expression.equals(that.expression)
                    && part == that.part;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "DATEPART(" + part + ", " + expression + ")";
    }

    @Override
    public String toVerboseString()
    {
        return "DATEPART(" + part + ", " + expression.toVerboseString() + ")";
    }

}
