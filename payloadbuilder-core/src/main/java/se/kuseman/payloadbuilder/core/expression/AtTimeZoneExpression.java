package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.EpochDateTime;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IAtTimeZoneExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** At time zone */
public class AtTimeZoneExpression implements IAtTimeZoneExpression
{
    private final IExpression expression;
    private final IExpression timeZone;

    public AtTimeZoneExpression(IExpression expression, IExpression timeZone)
    {
        this.expression = requireNonNull(expression, "expression");
        this.timeZone = requireNonNull(timeZone, "timeZone");
    }

    @Override
    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public IExpression getTimeZone()
    {
        return timeZone;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(expression, timeZone);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        final ValueVector value = expression.eval(input, context);
        final ValueVector timeZone = this.timeZone.eval(input, context);
        final String constantTimeZone = this.timeZone.isConstant() ? timeZone.getString(0)
                .toString()
                : null;

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
                        || timeZone.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || timeZone.isNull(row);
            }

            @Override
            public EpochDateTime getDateTime(int row)
            {
                String timeZoneString = constantTimeZone;
                if (timeZoneString == null)
                {
                    timeZoneString = timeZone.getString(row)
                            .toString();
                }

                ZoneId zone;
                try
                {
                    zone = ZoneId.of(timeZoneString);
                }
                catch (DateTimeException e)
                {
                    throw new IllegalArgumentException("Time zone '" + timeZoneString + "' could not be found.");
                }
                return value.getDateTime(row)
                        .atZone(zone);
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
        else if (obj instanceof AtTimeZoneExpression)
        {
            AtTimeZoneExpression that = (AtTimeZoneExpression) obj;
            return expression.equals(that.expression)
                    && timeZone.equals(that.timeZone);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression + " AT TIME ZONE " + timeZone;
    }

    @Override
    public String toVerboseString()
    {
        return expression.toVerboseString() + " AT TIME ZONE " + timeZone.toVerboseString();
    }
}
