package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.apache.commons.lang3.NotImplementedException;

public abstract class AJoin extends ASelectNode
{
    private final TableSource tableSource;

    public AJoin(TableSource tableSource)
    {
        this.tableSource = requireNonNull(tableSource, "tableSource");
    }

    public TableSource getTableSource()
    {
        return tableSource;
    }

    @Override
    public String toString()
    {
        throw new NotImplementedException(getClass().getSimpleName().toString());
    }
}
