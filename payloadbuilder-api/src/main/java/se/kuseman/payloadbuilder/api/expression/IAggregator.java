package se.kuseman.payloadbuilder.api.expression;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Definition of an aggregator. This is created once for each expression that is used during an aggregation session to keep state until stream is complete
 */
public interface IAggregator
{
    /**
     * Appends a group data to aggregator.
     *
     * @param input The input vector
     * @param groupIds Integer Vector with group ids
     * @param selections Vector with selections for each group. Type is Array[Int]. Same size as @param groupIds
     */
    void appendGroup(TupleVector input, ValueVector groupIds, ValueVector selections, IExecutionContext context);

    /**
     * Combine the aggregators state into a resulting ValueVector. The resulting vector must contain and be ordered by all the groupId's provided by
     * {@link #appendGroup(TupleVector, IExecutionContext)}
     */
    ValueVector combine(IExecutionContext context);

}