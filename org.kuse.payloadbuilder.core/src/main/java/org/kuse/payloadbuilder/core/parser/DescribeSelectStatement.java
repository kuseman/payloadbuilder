package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** DESCRIBE select .... */
public class DescribeSelectStatement extends Statement
{
    private final SelectStatement selectStatement;
    DescribeSelectStatement(SelectStatement selectStatement)
    {
        this.selectStatement = requireNonNull(selectStatement, "selectStatement");
    }

    public SelectStatement getSelectStatement()
    {
        return selectStatement;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
