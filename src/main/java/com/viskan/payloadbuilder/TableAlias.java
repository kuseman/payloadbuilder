package com.viskan.payloadbuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Domain of a table alias */
public class TableAlias
{
    private TableAlias parent;
    private String alias;
    private String table;
    private String[] columns;
    private List<TableAlias> childAliases;

    // Index of rows of this type in parent row
    int parentIndex;

    public String getTable()
    {
        return table;
    }

    public String getAlias()
    {
        return alias;
    }
    
    public TableAlias getParent()
    {
        return parent;
    }
    
    public TableAlias getChildAlias(String alias)
    {
        if (childAliases == null)
        {
            return null;
        }
        for (TableAlias child : childAliases)
        {
            if (Objects.equals(child.alias, alias))
            {
                return child;
            }
        }
        
        return null;
    }
    
    public String[] getColumns()
    {
        return columns;
    }

    public void setColumns(String[] columns)
    {
        this.columns = columns;
    }
    
    public int getParentIndex()
    {
        return parentIndex;
    }
    
    /** TODO: Should not be public */
    public void setParentIndex(int parentIndex)
    {
        this.parentIndex = parentIndex;
    }
    
    @Override
    public String toString()
    {
        return table + " ( " + alias + ")";
    }

    /** Construct a table meta from provided table name*/
    public static TableAlias of(TableAlias parent, String table, String alias)
    {
        TableAlias m = new TableAlias();
        m.parent = parent;
        m.table = table;
        m.alias = alias;
        
        if (parent != null)
        {
            if (parent.childAliases == null)
            {
                parent.childAliases = new ArrayList<>();
            }
            parent.childAliases.add(m);
            m.parentIndex = parent.childAliases.size() - 1;
        }
        
        return m;
    }
}
