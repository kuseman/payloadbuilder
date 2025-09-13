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
    /** Reference to the source table source reference. */
    private final TableSourceReference source;

    public TableSourceReference(int id, Type type, String catalogAlias, QualifiedName name, String alias)
    {
        this(id, type, catalogAlias, name, alias, null);
    }

    public TableSourceReference(int id, Type type, String catalogAlias, QualifiedName name, String alias, TableSourceReference source)
    {
        this.id = id;
        this.type = requireNonNull(type, "type");
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.name = requireNonNull(name, "name");
        this.alias = Objects.toString(alias, "");
        this.source = source;
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

    public TableSourceReference getSource()
    {
        return source;
    }

    /** Create a new instance with a link to provided table source reference. */
    public TableSourceReference withSource(TableSourceReference source)
    {
        if (requireNonNull(source).getId() == id)
        {
            return this;
        }
        return new TableSourceReference(id, type, catalogAlias, name, alias, source);
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
                    && Objects.equals(source, that.source);
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
