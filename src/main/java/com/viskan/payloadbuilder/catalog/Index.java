package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static java.util.Objects.requireNonNull;

import java.util.List;

/**
 * Index. Used on a table to define index which can be utilized by batching operators
 **/
public class Index
{
    private final QualifiedName table;
    private final List<String> columns;

    public Index(QualifiedName table, List<String> columns)
    {
        this.table = requireNonNull(table, "table");
        this.columns = requireNonNull(columns, "columns");
    }

    public List<String> getColumns()
    {
        return columns;
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
            Index i = (Index) obj;
            return columns.equals(i.columns);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return table + " " + columns.toString();
    }
}
