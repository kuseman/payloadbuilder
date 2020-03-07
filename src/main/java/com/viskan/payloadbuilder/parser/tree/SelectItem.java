package com.viskan.payloadbuilder.parser.tree;

public abstract class SelectItem extends ANode
{
    protected final String identifier;

    public SelectItem(String identifier)
    {
        this.identifier = identifier;
    }
    
    @Override
    public String toString()
    {
        return identifier != null ? (" \"" + identifier + "\"") : "";
    }
}
