package com.viskan.payloadbuilder.parser.tree;

/** Base class for select items */
public abstract class SelectItem extends ANode
{
    protected final String identifier;
    private final boolean explicitIdentifier;

    public SelectItem(String identifier, boolean explicitIdentifier)
    {
        this.identifier = identifier;
        this.explicitIdentifier = explicitIdentifier;
    }
    
    public boolean isExplicitIdentifier()
    {
        return explicitIdentifier;
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
