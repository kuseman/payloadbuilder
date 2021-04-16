package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Grouped row. Result of a {@link GroupByOperator} */
class GroupedRow implements Tuple
{
    private final int tupleOrdinal;
    private final List<Tuple> tuples;
    /** Set of column ordinals in group expressions, these columns should not return an aggregated value */
    private final Map<Integer, Set<Integer>> columnOrdinals;

    GroupedRow(List<Tuple> tuples, Map<Integer, Set<Integer>> columnOrdinals)
    {
        if (isEmpty(tuples))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.tuples = tuples;
        // Ordinal of a group by is the same as the rows
        this.tupleOrdinal = tuples.get(0).getTupleOrdinal();
        this.columnOrdinals = requireNonNull(columnOrdinals);
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        // Extract grouped ordinals for wanted ordinal
        Set<Integer> singleValueOrdinals = columnOrdinals.getOrDefault(ordinal, emptySet());
        // Return a collection tuple for the provided ordinal and group columns
        return new CollectionTuple(tuples, ordinal, singleValueOrdinals);
    }

    @Override
    public int getColumnCount()
    {
        return tuples.get(0).getColumnCount();
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return tuples.get(0).getColumnOrdinal(column);
    }

    @Override
    public String getColumn(int ordinal)
    {
        return tuples.get(0).getColumn(ordinal);
    }

    @Override
    public Object getValue(int ordinal)
    {
        return tuples.get(0).getValue(ordinal);
    }
}
