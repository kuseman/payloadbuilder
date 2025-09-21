package se.kuseman.payloadbuilder.core;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/**
 * Query result implementation for writing results to an {@link OutputWriter}.
 */
class QueryResultImpl extends AQueryResultImpl implements QueryResult
{
    QueryResultImpl(QuerySession session, QueryStatement queryStatement)
    {
        super(session, queryStatement);
    }

    @Override
    public void writeResult(OutputWriter writer)
    {
        processCurrentPlan(writer, null);
    }
}
