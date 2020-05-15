package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.operator.BatchOperator;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Index. 
 * Used on a table to define index which can be utilized by
 * a {@link BatchOperator}.
 **/
public class Index
{
    /** Columns in key */
    protected final List<String> columns;

    public Index(List<String> columns)
    {
        this.columns = requireNonNull(columns, "columns");
    }
    
    public List<String> getColumns()
    {
        return columns;
    }
    
    @Override
    public int hashCode()
    {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Index)
        {
            Index i = (Index) obj;
            return columns.equals(i.columns);
        }
        return false;
    }
    
    @Override
    public String toString()
    {
        return columns.toString();
    }
}
