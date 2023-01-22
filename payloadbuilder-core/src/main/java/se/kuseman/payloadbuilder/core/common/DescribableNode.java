package se.kuseman.payloadbuilder.core.common;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/**
 * Definition of a describable node. Used is describe/analyze statements to build a tree of information
 */
public interface DescribableNode
{
    /** Get id of this node */
    default int getNodeId()
    {
        return -1;
    }

    /**
     * Return the actual id of this node.
     *
     * <pre>
     * If this node is intercepted the {@link #getNodeId()} won't return
     * the "real" nodeId for this node but rather the intercepted nodes node id
     * </pre>
     */
    default int getActualNodeId()
    {
        return getNodeId();
    }

    /** Return child nodes if any */
    default List<DescribableNode> getChildNodes()
    {
        return emptyList();
    }

    /** Returns name of node */
    default String getName()
    {
        return getClass().getSimpleName();
    }

    /**
     * Returns a map with describe properties that is used during describe/analyze statements
     *
     * @param context Execution context
     */
    default Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return emptyMap();
    }
}
