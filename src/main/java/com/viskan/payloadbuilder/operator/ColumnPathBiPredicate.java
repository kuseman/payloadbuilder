package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.function.Predicate;

/** Bi predicate that compares to rows according to column paths */
class ColumnPathPredicate implements Predicate<Row>
{
    private final ColumnPathProjection outerProjection;
    private final ColumnPathProjection innerProjection;

    ColumnPathPredicate(String outerPath, String innerPath)
    {
        this.outerProjection = new ColumnPathProjection(requireNonNull(outerPath));
        this.innerProjection = new ColumnPathProjection(requireNonNull(innerPath));
    }

    @Override
    public boolean test(Row row)
    {
        Object outerValue = outerProjection.getValue(row);
        Object innerValue = innerProjection.getValue(row);
        return Objects.equals(outerValue, innerValue);
    }
}
