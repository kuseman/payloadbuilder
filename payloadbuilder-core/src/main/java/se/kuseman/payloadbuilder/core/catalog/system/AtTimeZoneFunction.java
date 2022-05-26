package se.kuseman.payloadbuilder.core.catalog.system;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Function that casts a Temporal to a another timezone */
class AtTimeZoneFunction extends ScalarFunctionInfo
{
    AtTimeZoneFunction(Catalog catalog)
    {
        super(catalog, "attimezone");
    }

    @Override
    public String getDescription()
    {
        return "Transforms datetime to another time zone." + System.lineSeparator() + "Ex. atTimeZone(<date expression>, <string timezone expression>) " + System.lineSeparator();
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object source = arguments.get(0)
                .eval(context);
        if (source == null)
        {
            return null;
        }

        String timeZone = (String) arguments.get(1)
                .eval(context);

        if (timeZone == null)
        {
            return null;
        }

        ZoneId zoneId = ZoneId.of(timeZone);

        if (source instanceof LocalDate)
        {
            // First create a system default zoned date time
            // then convert it to target zone
            LocalDate obj = (LocalDate) source;
            return obj.atTime(0, 0, 0)
                    .atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(zoneId);
        }
        else if (source instanceof LocalDateTime)
        {
            // First create a system default zoned date time
            // then convert it to target zone
            LocalDateTime obj = (LocalDateTime) source;
            return obj.atZone(ZoneId.systemDefault())
                    .withZoneSameInstant(zoneId);
        }
        else if (source instanceof ZonedDateTime)
        {
            ZonedDateTime obj = (ZonedDateTime) source;
            return obj.withZoneSameInstant(zoneId);
        }

        throw new IllegalArgumentException("Cannot change time zone of value: " + source);
    }
}
