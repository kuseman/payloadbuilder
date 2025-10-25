package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Drop table statement */
public class DropTableStatement extends Statement
{
    private final String catalogAlias;
    private final QualifiedName qname;
    private final boolean lenient;
    private final Location location;

    public DropTableStatement(String catalogAlias, QualifiedName qname, boolean lenient, Location location)
    {
        this.catalogAlias = catalogAlias;
        this.location = location;
        this.qname = requireNonNull(qname, "qname");
        this.lenient = lenient;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public boolean isLenient()
    {
        return lenient;
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
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
