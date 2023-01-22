package se.kuseman.payloadbuilder.core;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
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

    /** Compile query with provided catalog registry */
    public static CompiledQuery compile(CatalogRegistry registry, String query)
    {
        QuerySession sesion = new QuerySession(registry);
        return compile(sesion, query);
    }

    /**
     * Compile query with provided catalog registry. Produces a reusable query that can be executed.
     *
     * <pre>
     * Note! The query is dependent on the catalog registry and hence cannot be executed with a different
     * registry later on.
     * This method takes a {@link QuerySession} as input which can be used when a long running session is used for different queries.
     * For example cached metadata data can be reused for different compilations etc. using {@link IQuerySession#getGenericCache()}.
     * Also catalog properties used during compilation can be provided via {@link IQuerySession#setCatalogProperty}
     * </pre>
     */
    public static CompiledQuery compile(QuerySession session, String query)
    {
        List<CompiledQuery.Warning> warnings = new ArrayList<>();
        QueryStatement queryStatement = PARSER.parseQuery(query, warnings);
        queryStatement = StatementPlanner.plan(session, queryStatement);
        return new CompiledQuery(queryStatement, warnings);
    }
}
