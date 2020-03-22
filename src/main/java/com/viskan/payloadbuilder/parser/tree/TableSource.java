package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

import org.apache.commons.lang.NotImplementedException;

public abstract class TableSource extends ANode
{
    protected final String alias;
    private final boolean populating;
    
    public TableSource(String alias, boolean populating)
    {
        this.alias = requireNonNull(alias, "alias");
        this.populating = populating;
    }
    
    public String getAlias()
    {
        return alias;
    }
    
    public boolean isPopulating()
    {
        return populating;
    } 
    
    @Override
    public String toString()
    {
        throw new NotImplementedException(getClass().getSimpleName().toString());
    }
}
