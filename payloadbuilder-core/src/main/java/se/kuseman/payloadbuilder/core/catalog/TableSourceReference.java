package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** A reference to a table source (table or table-function) */
public class TableSourceReference
{
    private final int id;
    private final String catalogAlias;
    private final QualifiedName name;
    private final String alias;

    public TableSourceReference(int id, String catalogAlias, QualifiedName name, String alias)
    {
        this.id = id;
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.name = requireNonNull(name, "name");
        this.alias = Objects.toString(alias, "");
    }

    public int getId()
    {
        return id;
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

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof TableSourceReference that)
        {
            return id == that.id
                    && catalogAlias.equals(that.catalogAlias)
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
                : catalogAlias + "#") + name;
    }
}
