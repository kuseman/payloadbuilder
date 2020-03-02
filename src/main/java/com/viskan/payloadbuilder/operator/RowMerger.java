package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import java.util.function.BiFunction;

/** Merges outer and inner row */
public class RowMerger implements BiFunction<Row, Row, Row>
{
    /** Default merger. Merges inner row into outer */
    public static final RowMerger DEFAULT = new RowMerger(-1, false);
    /** Copy merger. Copied outer row before merge */
    public static final RowMerger COPY = new RowMerger(-1, true);

    /** Limit number of merged rows */
    private final int limit;
    /** Copy outer row before merge */
    private final boolean copy;

    public RowMerger(int limit, boolean copy)
    {
        this.limit = limit;
        this.copy = copy;
    }

    @Override
    public Row apply(Row outer, Row inner)
    {
        Row result = outer;
        if (copy)
        {
            result = new Row(result, inner.getPos());
        }
        result.merge(inner, limit);
        return result;
    }

    static RowMerger limit(int limit)
    {
        return new RowMerger(limit, false);
    }
}
