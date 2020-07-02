package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.QueryStatement;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    private Payloadbuilder()
    {
    }

    /** Perform query with provided session and query string */
    public static QueryResult query(QuerySession session, String queryString)
    {
        QueryStatement query = PARSER.parseQuery(queryString);
        return new QueryResultImpl(session, query);
    }
}
