package se.kuseman.payloadbuilder.api.execution;

import java.util.List;
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

    /** Return all node datas for a specific node class */
    <T extends NodeData> List<T> getNodeDatas(Class<T> nodeClass);
}
