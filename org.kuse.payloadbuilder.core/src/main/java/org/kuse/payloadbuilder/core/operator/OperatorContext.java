package org.kuse.payloadbuilder.core.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** Stores node unique data by node's unique id */
    private final Map<Integer, NodeData> nodeDataById = new ConcurrentHashMap<>();

    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<Object[]> outerIndexValues;

    public Iterator<Object[]> getOuterIndexValues()
    {
        return outerIndexValues;
    }

    public void setOuterIndexValues(Iterator<Object[]> outerIndexValues)
    {
        this.outerIndexValues = outerIndexValues;
    }

    /** Clear context state */
    public void clear()
    {
        outerIndexValues = null;
        nodeDataById.clear();
    }

    public Map<Integer, ? extends NodeData> getNodeData()
    {
        return nodeDataById;
    }

    /** Get node data by id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId)
    {
        return (T) nodeDataById.get(nodeId);
    }

    /** Get or create node data provided id */
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId, Supplier<T> creator)
    {
        return (T) nodeDataById.compute(nodeId, (k, v) ->
        {
            if (v == null)
            {
                v = creator.get();
            }
            v.executionCount++;
            return v;
        });
    }

    /** Base class for node data. */
    //CSOFF
    public static class NodeData
    {
        public int executionCount;
        /** Operator specific properties. Bytes fetched etc. */
        public Map<String, Object> properties = new HashMap<>();
    }
    //CSON
}
