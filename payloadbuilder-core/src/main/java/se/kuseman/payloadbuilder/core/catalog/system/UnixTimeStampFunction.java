package se.kuseman.payloadbuilder.core.catalog.system;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** unix_timestamp. Date to epuch millis */
class UnixTimeStampFunction extends ScalarFunctionInfo
{
    UnixTimeStampFunction(Catalog catalog)
    {
        super(catalog, "unix_timestamp");
    }

    @Override
    public DataType getDataType(List<? extends IExpression> arguments)
    {
        return DataType.LONG;
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object arg = arguments.get(0)
                .eval(context);
        if (arg == null)
        {
            return null;
        }
        if (arg instanceof LocalDateTime)
        {
            Instant instant = ((LocalDateTime) arg).atZone(ZoneId.systemDefault())
                    .toInstant();
            return instant.toEpochMilli();
        }
        else if (arg instanceof ZonedDateTime)
        {
            ZonedDateTime zonedDateTime = (ZonedDateTime) arg;
            return zonedDateTime.toInstant()
                    .toEpochMilli();
        }

        throw new IllegalArgumentException("Expected a Date to " + getName() + " but got: " + arg);
    }
}
