package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IDateAddExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Date add function expression */
public class DateAddExpression implements IDateAddExpression
{
    private final IDatePartExpression.Part part;
    private final IExpression number;
    private final IExpression expression;

    public DateAddExpression(DatePartExpression.Part part, IExpression number, IExpression expression)
    {
        this.expression = requireNonNull(expression, "expression");
        this.number = requireNonNull(number, "number");
        this.part = requireNonNull(part, "part");
    }

    @Override
    public DatePartExpression.Part getPart()
    {
        return part;
    }

    @Override
    public IExpression getNumber()
    {
        return number;
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(number, expression);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);
        final ValueVector number = this.number.eval(input, context);

        final ResolvedType type = getType();

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return type;
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || number.isNull(row);
            }

            @Override
            public EpochDateTimeOffset getDateTimeOffset(int row)
            {
                // Implicit cast
                if (type.getType() != Column.Type.DateTimeOffset)
                {
                    return ValueVector.super.getDateTimeOffset(row);
                }

                long add = number.getLong(row);
                return value.getDateTimeOffset(row)
                        .add(add, part.getChronoField()
                                .getBaseUnit());
            }

            @Override
            public EpochDateTime getDateTime(int row)
            {
                // Implicit cast
                if (type.getType() != Column.Type.DateTime)
                {
                    return ValueVector.super.getDateTime(row);
                }

                long add = number.getLong(row);
                return value.getDateTime(row)
                        .add(add, part.getChronoField()
                                .getBaseUnit());
            }
        };
    }

    @Override
    public ResolvedType getType()
    {
        ResolvedType type = expression.getType();

        // If we have an offset as input we keep it
        if (type.getType() == Column.Type.DateTimeOffset)
        {
            return type;
        }

        // ... all other is promoted to datetime
        return ResolvedType.of(Type.DateTime);
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
        else if (obj instanceof DateAddExpression)
        {
            DateAddExpression that = (DateAddExpression) obj;
            return expression.equals(that.expression)
                    && number.equals(that.number)
                    && part == that.part;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "DATEADD(" + part + ", " + number + ", " + expression + ")";
    }

    @Override
    public String toVerboseString()
    {
        return "DATEADD(" + part + ", " + number.toVerboseString() + ", " + expression.toVerboseString() + ")";
    }
}
