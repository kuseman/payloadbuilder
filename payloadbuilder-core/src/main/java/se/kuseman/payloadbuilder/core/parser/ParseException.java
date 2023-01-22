package se.kuseman.payloadbuilder.core.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.ObjectUtils;

/** Parse exception */
public class ParseException extends RuntimeException
{
    private final Location location;

    public ParseException(String message, ParserRuleContext ctx)
    {
        super(message);
        this.location = Location.from(ctx);
    }

    public ParseException(String message, Location location)
    {
        super(message);
        this.location = ObjectUtils.defaultIfNull(location, Location.EMPTY);
    }

    public Location getLocation()
    {
        return location;
    }
}
