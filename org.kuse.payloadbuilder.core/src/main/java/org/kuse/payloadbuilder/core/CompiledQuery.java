package org.kuse.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import org.kuse.payloadbuilder.core.parser.QueryStatement;

/**
 * Result of a query compilation. This class can be reused between executions
 *
 * <pre>
 * NOTE! The built query is compiled against a catalog registry that is assumed to
 * be the same during execution, else unexpected results will occur
 * </pre>
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
