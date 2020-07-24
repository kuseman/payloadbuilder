package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Select statement */
public class SelectStatement extends Statement
{
    private final Select select;
    SelectStatement(Select select)
    {
        this.select = requireNonNull(select, "select");
    }

    public Select getSelect()
    {
        return select;
    }
    
    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
