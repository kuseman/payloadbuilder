package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Objects;

/** Base class for table sources */
public abstract class TableSource implements ILogicalPlan
{
    /** Specified catalog alias */
    private final String catalogAlias;
    /** Table source alias */
    private final String alias;

    TableSource(String catalogAlias, String alias)
    {
        this.catalogAlias = Objects.toString(catalogAlias, "");
        this.alias = Objects.toString(alias, "");
    }

    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getAlias()
    {
        return alias;
    }

    @Override
    public List<ILogicalPlan> getChildren()
    {
        return emptyList();
    }

    @Override
    public abstract int hashCode();

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof TableSource)
        {
            TableSource that = (TableSource) obj;
            return catalogAlias.equals(that.catalogAlias)
                    && alias.equals(that.alias);
        }
        return false;
    }
}
