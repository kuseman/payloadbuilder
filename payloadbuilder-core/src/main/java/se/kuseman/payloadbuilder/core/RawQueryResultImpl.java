package se.kuseman.payloadbuilder.core;

import java.util.function.Function;

import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/**
 * Query result implementation for returning raw {@link TupleVector}.
 */
class RawQueryResultImpl extends AQueryResultImpl implements RawQueryResult
{
    RawQueryResultImpl(QuerySession session, QueryStatement queryStatement)
    {
        super(session, queryStatement);
    }

    @Override
    public void consumeResult(Function<TupleVector, Boolean> consumer)
    {
        processCurrentPlan(null, consumer);
    }
}
