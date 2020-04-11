package com.viskan.payloadbuilder;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.List;

import org.apache.commons.collections.iterators.TransformIterator;

/** Grouped row */
public class GroupedRow extends Row
{
    private final List<Row> rows;

    public GroupedRow(List<Row> rows, int pos)
    {
        if (isEmpty(rows))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.rows = rows;
        this.pos = pos;
        super.tableAlias = rows.get(0).getTableAlias();
    }
    
    @Override
    public Object getObject(int ordinal)
    {
        return (Iterable) () -> new TransformIterator(rows.iterator(), row -> ((Row) row).getObject(ordinal));
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof GroupedRow)
        {
            // Assume grouped row
            return ((GroupedRow) obj).pos == pos;
        }
        return false;
    }
}
