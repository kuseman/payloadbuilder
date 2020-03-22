package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Map;

import gnu.trove.map.hash.THashMap;

/** Context used during selection of operator tree */
public class OperatorContext
{
    /** Spool storage */
    Map<String, List<Row>> spooledRowsByKey = new THashMap<>();
    
    /** Reference to parent row during selection inside projections */
    private Row parentProjectionRow;
 
    /** Lookup index values by key
     * TODO: need qualified name here later on
     *  */
    private Map<String, List<Object>> indexLookupValues;
    
    /** Store spool rows with key */
    public void storeSpoolRows(String key, List<Row> rows)
    {
        spooledRowsByKey.put(key, rows);
    }
    
    public List<Row> getSpoolRows(String key)
    {
        return spooledRowsByKey.getOrDefault(key, emptyList());
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
