package org.kuse.payloadbuilder.core.parser;

/** Base class for select items */
//CSOFF
public abstract class SelectItem extends ASelectNode
//CSON
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

    /** Name of assignment variable if any */
    public String getAssignmentName()
    {
        return null;
    }

    /**
     * Calculate assignment value if any
     *
     * @param context Execution context
     */
    public Object getAssignmentValue(ExecutionContext context)
    {
        return null;
    }

    @Override
    public String toString()
    {
        return identifier != null ? (" \"" + identifier + "\"") : "";
    }
}
