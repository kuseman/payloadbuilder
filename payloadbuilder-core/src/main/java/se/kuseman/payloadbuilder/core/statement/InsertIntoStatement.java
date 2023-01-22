package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.core.common.Option;

/** Logical insert into statement. Note! only insert into in memory temp table are supported for now. */
public class InsertIntoStatement extends Statement
{
    private final SelectStatement selectStatement;
    private final String table;
    private final List<Option> options;

    public InsertIntoStatement(SelectStatement selectStatement, String table, List<Option> options)
    {
        this.selectStatement = requireNonNull(selectStatement, "selectStatement");
        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
    }

    public SelectStatement getSelectStatement()
    {
        return selectStatement;
    }

    public String getTable()
    {
        return table;
    }

    public List<Option> getOptions()
    {
        return options;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
