package se.kuseman.payloadbuilder.core.statement;

import se.kuseman.payloadbuilder.core.parser.Location;

/** Show statement for querying current parameters/variables */
public class ShowStatement extends Statement
{
    private final Type type;
    private final String catalog;
    private final Location location;

    public ShowStatement(Type type, String catalog, Location location)
    {
        this.type = type;
        this.catalog = catalog;
        this.location = location;
    }

    public Type getType()
    {
        return type;
    }

    public String getCatalog()
    {
        return catalog;
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

    /** Type */
    public enum Type
    {
        VARIABLES,
        TABLES,
        FUNCTIONS,
        CACHES
    }
}
