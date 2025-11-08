package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** A reference to a table source (table or table-function) */
public class TableSourceReference
{
    /** Id of table source to make it unique in cases where the same ref is used on different places in query tree */
    private final int id;
    private final Type type;
    private final String catalogAlias;
    private final QualifiedName name;
    private final String alias;
    /** Reference to the parent table source reference. */
    private final TableSourceReference parent;

    public TableSourceReference(int id, Type type, String catalogAlias, QualifiedName name, String alias)
    {
        this(id, type, catalogAlias, name, alias, null);
    }

    public TableSourceReference(int id, Type type, String catalogAlias, QualifiedName name, String alias, TableSourceReference parent)
    {
        this.id = id;
        this.type = requireNonNull(type, "type");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.name = requireNonNull(name, "name");
        this.alias = Objects.toString(alias, "");
        this.parent = parent;
    }

    public int getId()
    {
        return id;
    }

    public Type getType()
    {
        return type;
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

    public TableSourceReference getParent()
    {
        return parent;
    }

    /** Create a new instance with a link to provided parent table source reference. */
    public TableSourceReference withParent(TableSourceReference parent)
    {
        // Parent is the same as this
        if (requireNonNull(parent).getId() == id)
        {
            return this;
        }
        // This parent is already the input parent
        else if (this.parent != null
                && this.parent.id == parent.id)
        {
            return this;
        }

        return new TableSourceReference(id, type, catalogAlias, name, alias, parent);
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
                    && type == that.type
                    && catalogAlias.equals(that.catalogAlias)
                    && alias.equals(that.alias)
                    && name.equals(that.name)
                    && Objects.equals(parent, that.parent);
        }
        return false;
    }

    @Override
    public String toString()
    {
        // es#table
        return ("".equals(catalogAlias) ? ""
                : catalogAlias + "#")
               + (isBlank(alias) ? ""
                       : alias + " -> ")
               + name
               + "("
               + id
               + ")";
    }

    /** Type of table source */
    public enum Type
    {
        TABLE,
        FUNCTION,
        EXPRESSION,
        SUBQUERY
    }
}
