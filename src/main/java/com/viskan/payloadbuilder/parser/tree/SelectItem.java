package com.viskan.payloadbuilder.parser.tree;

/** Base class for select items */
public abstract class SelectItem extends ANode
{
    protected final String identifier;

    public SelectItem(String identifier)
    {
        this.identifier = identifier;
    }
    
    public String getIdentifier()
    {
        return identifier;
    }
    
    @Override
    public String toString()
    {
        return identifier != null ? (" \"" + identifier + "\"") : "";
    }
}
