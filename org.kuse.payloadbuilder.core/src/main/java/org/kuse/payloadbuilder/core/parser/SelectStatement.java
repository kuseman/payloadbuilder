package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Select statement */
public class SelectStatement extends Statement
{
    private final Select select;
    private final boolean assignmentSelect;

    SelectStatement(Select select, boolean assignmentSelect)
    {
        this.select = requireNonNull(select, "select");
        this.assignmentSelect = assignmentSelect;
    }

    public Select getSelect()
    {
        return select;
    }

    public boolean isAssignmentSelect()
    {
        return assignmentSelect;
    }
    
    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
