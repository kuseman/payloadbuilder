package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;

/** Base class for table sources */
public abstract class TableSource implements ILogicalPlan
{
    protected final TableSourceReference tableSource;

    TableSource(TableSourceReference tableSource)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
    }

    public String getCatalogAlias()
    {
        return tableSource.getCatalogAlias();
    }

    public String getAlias()
    {
        return tableSource.getAlias();
    }

    public TableSourceReference getTableSource()
    {
        return tableSource;
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
        else if (obj instanceof TableSource that)
        {
            return tableSource.equals(that.tableSource);
        }
        return false;
    }
}
