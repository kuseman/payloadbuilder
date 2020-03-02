package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.function.ToIntFunction;

/** ToInt function that used for eg. hashing a column */
class ColumnPathHashFunction implements ToIntFunction<Row>
{
    private final ColumnPathProjection columnPathProjection;
    private final String path;

    ColumnPathHashFunction(String path)
    {
        this.path = requireNonNull(path);
        this.columnPathProjection = new ColumnPathProjection(path);
    }

    @Override
    public int applyAsInt(Row row)
    {
        Object value = columnPathProjection.getValue(row);
        // Integer optimization to avoid a method dispatch
        if (value instanceof Integer)
        {
            return ((Integer) value).intValue();
        }
        return value != null ? Objects.hash(value) : 0;
    }

    @Override
    public String toString()
    {
        return path;
    }
}
