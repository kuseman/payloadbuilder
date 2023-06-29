package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.core.common.Option;

/** Logical insert into statement. Note! only insert into in memory temp table are supported for now. */
public class InsertIntoStatement extends LogicalSelectStatement
{
    public static final String INDICES = "indices";

    private final String table;
    private final List<Option> options;
    private final Token token;

    public InsertIntoStatement(LogicalSelectStatement selectStatement, String table, List<Option> options, Token token)
    {
        super(selectStatement.getSelect(), false);
        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
        this.token = token;
    }

    public String getTable()
    {
        return table;
    }

    public List<Option> getOptions()
    {
        return options;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
