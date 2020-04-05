package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Map;

import gnu.trove.map.hash.THashMap;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** Context used when evaluating expressions */
    private final EvaluationContext evaluationContext = new EvaluationContext();
    
    /** Spool storage */
    private final Map<String, List<Row>> spooledRowsByKey = new THashMap<>();
    
    /** Reference to parent row. Used in projections, correlated sub queries */
    private Row parentRow;
 
    /** Store spool rows with key */
    public void storeSpoolRows(String key, List<Row> rows)
    {
        spooledRowsByKey.put(key, rows);
    }
    
    public List<Row> getSpoolRows(String key)
    {
        return spooledRowsByKey.getOrDefault(key, emptyList());
    }
    
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
}
