package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Logical insert into statement. */
public class LogicalInsertIntoStatement extends LogicalSelectIntoStatement
{
    private final List<String> columns;

    public LogicalInsertIntoStatement(LogicalSelectStatement input, String catalogAlias, QualifiedName table, List<String> columns, List<Option> options, Location location)
    {
        super(input, catalogAlias, table, options, location);
        this.columns = requireNonNull(columns, "columns");
    }

    public List<String> getColumns()
    {
        return columns;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
