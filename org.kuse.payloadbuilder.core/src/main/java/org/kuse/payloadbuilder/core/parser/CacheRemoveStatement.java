package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Cache remove statement */
public class CacheRemoveStatement extends Statement
{
    private final String name;
    public CacheRemoveStatement(String name)
    {
        this.name = requireNonNull(name, "name");
    }

    public String getName()
    {
        return name;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
