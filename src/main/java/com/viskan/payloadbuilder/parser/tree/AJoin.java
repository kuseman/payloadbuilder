package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

import org.apache.commons.lang.NotImplementedException;

public abstract class AJoin extends ANode
{
    private final TableSource tableSource;
    
    public AJoin(TableSource tableSource)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
    }
    
    public TableSource getTableSource()
    {
        return tableSource;
    }
    
    @Override
    public String toString()
    {
        throw new NotImplementedException(getClass().getSimpleName().toString());
    }
}
