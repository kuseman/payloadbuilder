package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.function.BiPredicate;

/** Bi predicate that compares to rows according to column paths */
class ColumnPathBiPredicate implements BiPredicate<Row, Row>
{
    private final ColumnPathProjection outerProjection;
    private final ColumnPathProjection innerProjection;

    ColumnPathBiPredicate(String outerPath, String innerPath)
    {
        this.outerProjection = new ColumnPathProjection(requireNonNull(outerPath));
        this.innerProjection = new ColumnPathProjection(requireNonNull(innerPath));
    }

    @Override
    public boolean test(Row outer, Row inner)
    {
        Object outerValue = outerProjection.getValue(outer);
        Object innerValue = innerProjection.getValue(inner);
        return Objects.equals(outerValue, innerValue);
    }
}
