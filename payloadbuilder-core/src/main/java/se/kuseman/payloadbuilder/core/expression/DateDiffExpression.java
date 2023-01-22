package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IDateDiffExpression;
import se.kuseman.payloadbuilder.api.expression.IDatePartExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** Date diff function expression */
public class DateDiffExpression implements IDateDiffExpression
{
    private final IDatePartExpression.Part part;
    private final IExpression start;
    private final IExpression end;

    public DateDiffExpression(DatePartExpression.Part part, IExpression start, IExpression end)
    {
        this.start = requireNonNull(start, "start");
        this.end = requireNonNull(end, "end");
        this.part = requireNonNull(part, "part");
    }

    @Override
    public DatePartExpression.Part getPart()
    {
        return part;
    }

    @Override
    public IExpression getStart()
    {
        return start;
    }

    @Override
    public IExpression getEnd()
    {
        return end;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(start, end);
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.Long);
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector start = this.start.eval(input, context);
        final ValueVector end = this.end.eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Long);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return start.isNullable()
                        || end.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return start.isNull(row)
                        || end.isNull(row);
            }

            @Override
            public long getLong(int row)
            {
                ZonedDateTime startDateTime = start.getDateTime(row)
                        .toZonedDateTime();
                ZonedDateTime endDateTime = end.getDateTime(row)
                        .toZonedDateTime();
                return part.getChronoField()
                        .getBaseUnit()
                        .between(startDateTime, endDateTime);
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }

    @Override
    public int hashCode()
    {
        return start.hashCode();
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
        else if (obj instanceof DateDiffExpression)
        {
            DateDiffExpression that = (DateDiffExpression) obj;
            return start.equals(that.start)
                    && end.equals(that.end)
                    && part == that.part;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "DATEDIFF(" + part + ", " + start + ", " + end + ")";
    }

    @Override
    public String toVerboseString()
    {
        return "DATEDIFF(" + part + ", " + start.toVerboseString() + ", " + end.toVerboseString() + ")";
    }
}
