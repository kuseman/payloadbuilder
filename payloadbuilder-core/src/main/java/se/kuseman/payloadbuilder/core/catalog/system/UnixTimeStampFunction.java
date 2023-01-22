package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** unix_timestamp. Date to epuch millis */
class UnixTimeStampFunction extends ScalarFunctionInfo
{
    UnixTimeStampFunction(Catalog catalog)
    {
        super(catalog, "unix_timestamp", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.Long);
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);

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
                return value.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public long getLong(int row)
            {
                return value.getDateTime(row)
                        .getEpoch();
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
    //
    // @Override
    // public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    // {
    // Object arg = arguments.get(0)
    // .eval(context);
    // if (arg == null)
    // {
    // return null;
    // }
    // if (arg instanceof LocalDateTime)
    // {
    // Instant instant = ((LocalDateTime) arg).atZone(ZoneId.systemDefault())
    // .toInstant();
    // return instant.toEpochMilli();
    // }
    // else if (arg instanceof ZonedDateTime)
    // {
    // ZonedDateTime zonedDateTime = (ZonedDateTime) arg;
    // return zonedDateTime.toInstant()
    // .toEpochMilli();
    // }
    //
    // throw new IllegalArgumentException("Expected a Date to " + getName() + " but got: " + arg);
    // }
}
