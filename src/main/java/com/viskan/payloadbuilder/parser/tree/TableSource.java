package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

import org.apache.commons.lang3.NotImplementedException;

/** Base class for table sources */
public abstract class TableSource extends ANode
{
    protected final String alias;
    
    public TableSource(String alias)
    {
        this.alias = requireNonNull(alias, "alias");
    }
    
    public String getAlias()
    {
        return alias;
    }
    
    @Override
    public String toString()
    {
        throw new NotImplementedException(getClass().getSimpleName().toString());
    }
}
