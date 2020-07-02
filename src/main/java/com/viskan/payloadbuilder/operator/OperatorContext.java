package com.viskan.payloadbuilder.operator;

import java.util.Iterator;
import java.util.function.Supplier;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/** Context used during selection of operator tree */
public class OperatorContext
{
//    private final QuerySession session;
    
    /** Context used when evaluating expressions */
//    private final EvaluationContext evaluationContext = new EvaluationContext();
    
    /** Stores node unique data by node's unique id */
    private final TIntObjectMap<Object> nodeDataById = new TIntObjectHashMap<>();
    
    /** Reference to parent row. Used in projections, correlated sub queries */
//    private Row parentRow;
    
    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<Object[]> outerIndexValues;

    public OperatorContext(/*QuerySession session*/)
    {
//        this.session = requireNonNull(session, "session");
    }
//    
//    public QuerySession getSession()
//    {
//        return session;
//    }
    
//    public Row getParentRow()
//    {
//        return parentRow;
//    }
//
//    public void setParentRow(Row parentRow)
//    {
//        this.parentRow = parentRow;
//    }
    
//    public EvaluationContext getEvaluationContext()
//    {
//        return evaluationContext;
//    }
    
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
    
    @SuppressWarnings("unchecked")
    public <T extends NodeData> T getNodeData(int nodeId, Supplier<T> creator)
    {
        Object data = nodeDataById.get(nodeId);
        if (data == null)
        {
            data = creator.get();
            nodeDataById.put(nodeId, data);
        }
        
        T result = (T) data;
        result.executionCount++;
        return result;
    }
    
    /** Base class for node data. */
    public static class NodeData
    {
        int executionCount;
    }
}
