package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import java.util.List;

/** Merges outer and inner row */
public class DefaultRowMerger implements RowMerger
{
    /** Default merger. Merges inner row into outer */
    public static final DefaultRowMerger DEFAULT = new DefaultRowMerger(-1);

    /** Limit number of merged rows */
    private final int limit;

    public DefaultRowMerger(int limit)
    {
        this.limit = limit;
    }

    @Override
    public Row merge(Row outer, Row inner, boolean populating)
    {
        Row result = outer;
        
        // No populating join, create a copy of outer row
        // and re-add it to parents before merging inner row
        if (!populating)
        {
            result = new Row(result, inner.getPos());
            int aliasIndex = outer.getTableAlias().getParentIndex();
            for (Row p : outer.getParents())
            {
                List<Row> childRows = p.getChildRows(aliasIndex);
                childRows.add(result);
            }
        }
        
        result.merge(inner, limit);
        return result;
    }

    /** Create a limiting row merger */
    static DefaultRowMerger limit(int limit)
    {
        return new DefaultRowMerger(limit);
    }
}
