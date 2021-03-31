package org.kuse.payloadbuilder.core.parser;

/** Analyze statement */
public class AnalyzeStatement extends Statement
{
    private final SelectStatement selectStatement;

    AnalyzeStatement(SelectStatement selectStatement)
    {
        this.selectStatement = selectStatement;
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
