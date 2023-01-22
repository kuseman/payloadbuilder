package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Logical insert into statement. Note! only insert into in memory temp table are supported for now. */
public class InsertIntoStatement extends LogicalSelectStatement
{
    public static final QualifiedName INDICES = QualifiedName.of("indices");

    private final QualifiedName table;
    private final List<Option> options;
    private final Location location;

    public InsertIntoStatement(LogicalSelectStatement selectStatement, QualifiedName table, List<Option> options, Location location)
    {
        super(selectStatement.getSelect(), false);
        this.table = requireNonNull(table, "table");
        this.options = requireNonNull(options, "options");
        this.location = location;
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
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
