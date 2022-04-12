package se.kuseman.payloadbuilder.core;

import se.kuseman.payloadbuilder.core.parser.QueryParser;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    private Payloadbuilder()
    {
    }

    /** Compile provided query qith with provided catgory registry */
    public static CompiledQuery compile(String query)
    {
        return new CompiledQuery(PARSER.parseQuery(query));
    }
}
