package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.StringUtils.defaultString;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference.Type;

/** A reference to a table source (table or table-function) */
public class TableSourceReference
{
    private final String catalogAlias;
    private final QualifiedName name;
    private final String alias;

    public TableSourceReference(String catalogAlias, QualifiedName name, String alias)
    {
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.name = requireNonNull(name, "name");
        this.alias = defaultString(alias, "");
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getAlias()
    {
        return alias;
    }

    public QualifiedName getName()
    {
        return name;
    }

    /** Construct a regular column reference from this table source */
    public ColumnReference column(String column)
    {
        return new ColumnReference(this, column, Type.REGULAR);
    }
    //
    // /** Construct a named asterisk column from this table source */
    // public ColumnReference asterisk(String column)
    // {
    // return new ColumnReference(this, column, true).of(column);
    // }
    //
    // /** Construct an asterisk column from this table source */
    // public ColumnReference asterisk()
    // {
    // return new ColumnReference(this, "", true);
    // }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        if (obj instanceof TableSourceReference)
        {
            TableSourceReference that = (TableSourceReference) obj;
            return catalogAlias.equals(that.catalogAlias)
                    && alias.equals(that.alias)
                    && name.equals(that.name);
        }
        return false;
    }

    @Override
    public String toString()
    {
        // es#table
        return ("".equals(catalogAlias) ? ""
                : catalogAlias + "#") + name.toDotDelimited();
    }

    public static TableSourceReference of(String name, String alias)
    {
        return of("", name, alias);
    }

    public static TableSourceReference of(String catalogAlias, String name, String alias)
    {
        return new TableSourceReference(catalogAlias, QualifiedName.of(name), alias);
    }
}
