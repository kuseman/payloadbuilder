package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import java.util.List;
import java.util.Map;

import gnu.trove.map.hash.THashMap;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** When using populating join, the current parent rows are provided here for
     * child operators to utilize */
    private List<Row> parentRows;
    
    /** Reference to parent row during selection inside projections */
    private Row parentProjectionRow;
 
    /** Lookup index values by key
     * TODO: need qualified name here later on
     *  */
    private Map<String, List<Object>> indexLookupValues;
    
    public List<Row> getParentRows()
    {
        return parentRows;
    }
    
    public void setParentRows(List<Row> parentRows)
    {
        this.parentRows = parentRows;
    }
    
    public Row getParentProjectionRow()
    {
        return parentProjectionRow;
    }
    
    public void setParentProjectionRow(Row parentProjectionRow)
    {
        this.parentProjectionRow = parentProjectionRow;
    }
    
    public List<Object> getIndexLookupValues(String key)
    {
        return indexLookupValues != null ? indexLookupValues.get(key) : null;
    }
    
    public void addIndexLookupValues(String key, List<Object> values)
    {
        if (indexLookupValues == null)
        {
            indexLookupValues = new THashMap<>();
        }
        
        indexLookupValues.put(key, values);
    }
}
