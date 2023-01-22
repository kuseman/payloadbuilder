package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.EpochDateTime;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IDateAddExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

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
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);
        final ValueVector number = this.number.eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.DateTime);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable()
                        || number.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || number.isNull(row);
            }

            @Override
            public EpochDateTime getDateTime(int row)
            {
                EpochDateTime dateTime = value.getDateTime(row);
                int add = number.getInt(row);

                return dateTime.add(add, part.getChronoField()
                        .getBaseUnit());
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
