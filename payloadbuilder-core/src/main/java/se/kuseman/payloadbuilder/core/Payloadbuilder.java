package se.kuseman.payloadbuilder.core;

import se.kuseman.payloadbuilder.core.parser.QueryParser;
import se.kuseman.payloadbuilder.core.planning.StatementPlanner;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    private Payloadbuilder()
    {
    }

    /**
     * Compile provided query qith with provided catgory registry. Produces a reusable query that can be executed. Note! The query is dependent on the catalog registry and hence cannot be executed
     * with a different registret later on.
     */
    public static CompiledQuery compile(String query, QuerySession session, String defaultCatalogAlias)
    {
        QueryStatement queryStatement = PARSER.parseQuery(session, query);
        queryStatement = StatementPlanner.plan(session, defaultCatalogAlias, queryStatement);
        return new CompiledQuery(queryStatement);
    }
}
