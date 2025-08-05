package se.kuseman.payloadbuilder.core.parser;

import static org.apache.commons.lang3.ObjectUtils.getIfNull;

import org.antlr.v4.runtime.ParserRuleContext;

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
        this.location = getIfNull(location, Location.EMPTY);
    }

    public Location getLocation()
    {
        return location;
    }
}
