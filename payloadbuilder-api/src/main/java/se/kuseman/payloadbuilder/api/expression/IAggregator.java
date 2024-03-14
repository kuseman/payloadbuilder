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
     * <pre>
     * Schema of group data:
     * 
     *  groupTables: Table
     *   - Table with the input schema but only with rows belonging to it's group
     *  groupIds:    Int
     *   - The unique id for each group corresponding to each table in 'groupTables' column
     * 
     * </pre>
     */
    void appendGroup(TupleVector groupData, IExecutionContext context);

    /**
     * Combine the aggregators state into a resulting ValueVector. The resulting vector must contain and be ordered by all the groupId's provided by
     * {@link #appendGroup(TupleVector, IExecutionContext)}
     */
    ValueVector combine(IExecutionContext context);
}