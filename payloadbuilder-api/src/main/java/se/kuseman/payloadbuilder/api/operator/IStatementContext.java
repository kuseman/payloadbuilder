package se.kuseman.payloadbuilder.api.operator;

import java.time.ZonedDateTime;
import java.util.function.Supplier;

/** Definition of a statement context. */
public interface IStatementContext
{
    /** Get node data for provided node id */
    <T extends NodeData> T getNodeData(Integer nodeId);

    /** Get or create node data for provided node id */
    <T extends NodeData> T getOrCreateNodeData(Integer nodeId);

    /** Get or create node data for provided node id with supplier */
    <T extends NodeData> T getOrCreateNodeData(Integer nodeId, final Supplier<T> creator);

    // /** Get lambda value for provided id */
    // Object getLambdaValue(int lambdaId);
    //
    // /** Set lambda value for provided id */
    // void setLambdaValue(int lambdaId, Object value);
    //
    // /** Get the current tuple in the context */
    // Tuple getTuple();

    /** Return current time in local time */
    ZonedDateTime getNow();
}
