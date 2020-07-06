package com.viskan.payloadbuilder.operator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** Stores node unique data by node's unique id */
    private final TIntObjectMap<NodeData> nodeDataById = new TIntObjectHashMap<>();
    
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
    
    public void clear()
    {
        outerIndexValues = null;
        nodeDataById.clear();       
    }
    
    public TIntObjectMap<? extends NodeData> getNodeData()
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
        T data = (T) nodeDataById.get(nodeId);
        if (data == null)
        {
            data = creator.get();
            nodeDataById.put(nodeId, data);
        }
        
        data.executionCount++;
        return data;
    }
    
    /** Base class for node data. */
    public static class NodeData
    {
        public int executionCount;
        /** Operator specific properties. Bytes fetched etc. */
        public Map<String, Object> properties = new HashMap<>();
    }
}
