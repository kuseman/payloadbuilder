package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Logical insert into statement. */
public class LogicalSelectIntoStatement extends Statement
{
    private final LogicalSelectStatement input;
    private final String catalogAlias;
    private final QualifiedName table;
    private final List<Option> options;
    private final Location location;

    public LogicalSelectIntoStatement(LogicalSelectStatement input, String catalogAlias, QualifiedName table, List<Option> options, Location location)
    {
        this.input = requireNonNull(input, "input");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
        this.location = location;
    }

    public LogicalSelectStatement getInput()
    {
        return input;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public List<Option> getOptions()
    {
        return options;
    }

    public Location getLocation()
    {
        return location;
    }

    @Override
    public List<Statement> getChildren()
    {
        return List.of(input);
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
