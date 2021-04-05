package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Base class for select items */
//CSOFF
public abstract class SelectItem extends ASelectNode
//CSON
{
    protected final String identifier;
    private final Token token;
    private final boolean emptyIdentifier;

    public SelectItem(String identifier, boolean emptyIdentifier, Token token)
    {
        this.emptyIdentifier = emptyIdentifier;
        this.identifier = requireNonNull(identifier, "identifier");
        this.token = token;
    }

    /** Returns true if this items identifier was defined empty */
    boolean isEmptyIdentifier()
    {
        return emptyIdentifier;
    }

    /** Get identifier for this select item */
    public String getIdentifier()
    {
        return identifier;
    }

    public Token getToken()
    {
        return token;
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
