package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/** Context used during selection of operator tree */
public class OperatorContext
{
    public Map<String, Object> data = new HashMap<>();
    
    public OperatorContext()
    {}
    
    public OperatorContext(Map<String, Object> params)
    {
        evaluationContext.setParameters(params);
    }
    
    /** Context used when evaluating expressions */
    private final EvaluationContext evaluationContext = new EvaluationContext();
    
    /** Reference to parent row. Used in projections, correlated sub queries */
    private Row parentRow;
    
    /** Hint for table operator to use index. Used in conjunction with {@link #outerIndexValues} */
    private Index index;
 
    /** Iterator of outer row values used when having an indexed inner operator in Batched operators */
    private Iterator<Object[]> outerIndexValues;
    
    public Row getParentRow()
    {
        return parentRow;
    }

    public void setParentRow(Row parentRow)
    {
        this.parentRow = parentRow;
    }
    
    public EvaluationContext getEvaluationContext()
    {
        return evaluationContext;
    }
    
    public Index getIndex()
    {
        return index;
    }
    
    public Iterator<Object[]> getOuterIndexValues()
    {
        return outerIndexValues;
    }
    
    public void setIndex(Index index, Iterator<Object[]> outerIndexValues)
    {
        this.index = index;
        this.outerIndexValues = outerIndexValues;
    }
}
