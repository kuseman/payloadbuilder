package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** unix_timestamp. Date to epuch millis */
class UnixTimeStampFunction extends ScalarFunctionInfo
{
    UnixTimeStampFunction(Catalog catalog)
    {
        super(catalog, "unix_timestamp");
    }

    @Override
    public Class<?> getDataType()
    {
        return Long.class;
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object arg = arguments.get(0).eval(context);
        if (arg == null)
        {
            return null;
        }
        if (arg instanceof LocalDateTime)
        {
            Instant instant = ((LocalDateTime) arg).atZone(ZoneId.systemDefault()).toInstant();
            return instant.toEpochMilli();
        }
        else if (arg instanceof ZonedDateTime)
        {
            ZonedDateTime zonedDateTime = (ZonedDateTime) arg;
            return zonedDateTime.toInstant().toEpochMilli();
        }

        throw new IllegalArgumentException("Expected a Date to " + getName() + " but got: " + arg);
    }
}
