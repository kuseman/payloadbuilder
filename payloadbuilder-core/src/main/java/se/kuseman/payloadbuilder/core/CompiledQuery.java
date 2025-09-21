package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/**
 * Result of a query compilation. This class can be reused between executions
 */
public class CompiledQuery
{
    private final QueryStatement query;
    private final List<Warning> warnings;

    CompiledQuery(QueryStatement query, List<Warning> warnings)
    {
        this.query = requireNonNull(query);
        this.warnings = warnings;
    }

    /** Execute this query with provided session */
    public QueryResult execute(QuerySession session)
    {
        return new QueryResultImpl(session, query);
    }

    /** Execute this query with provided session returning a raw query result that returns the raw {@link TupleVector}'s */
    public RawQueryResult executeRaw(QuerySession session)
    {
        return new RawQueryResultImpl(session, query);
    }

    public List<Warning> getWarnings()
    {
        return warnings;
    }

    /** A compiler warning */
    public static record Warning(String message, Location location)
    {
    }
}
