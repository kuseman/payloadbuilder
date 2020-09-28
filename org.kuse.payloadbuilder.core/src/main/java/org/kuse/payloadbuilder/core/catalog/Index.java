package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * A table index. Defines columns that can be utilized in batching operators for quicker access to rows.
 **/
public class Index
{
    private final QualifiedName table;
    private final List<String> columns;
    private final int batchSize;

    public Index(QualifiedName table, List<String> columns, int batchSize)
    {
        this.table = requireNonNull(table, "table");
        this.columns = requireNonNull(columns, "columns");
        this.batchSize = batchSize;
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public int getBatchSize()
    {
        return batchSize;
    }

    @Override
    public int hashCode()
    {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Index)
        {
            Index that = (Index) obj;
            return columns.equals(that.columns)
                && batchSize == that.batchSize;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return table + " " + columns.toString();
    }
}
