package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/**
 * Result of a query compilation. This class can be reused between executions
 */
public class CompiledQuery
{
    private final QueryStatement query;

    CompiledQuery(QueryStatement query)
    {
        this.query = requireNonNull(query);
    }

    /** Execute this query with provided session */
    public QueryResult execute(QuerySession session)
    {
        return new QueryResultImpl(session, query);
    }
}
