package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.iterators.TransformIterator;

/** Grouped row. Result of a {@link GroupByOperator} */
class GroupedRow implements Tuple
{
    private final int tupleOrdinal;
    private final List<Tuple> tuples;
    /** Set of columns in group expressions, these columns should not return an aggregated value */
    private final Map<Integer, Set<String>> columnReferences;

    GroupedRow(List<Tuple> tuples, Map<Integer, Set<String>> columnReferences)
    {
        if (isEmpty(tuples))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.tuples = tuples;
        // Ordinal of a group by is the same as the rows
        this.tupleOrdinal = tuples.get(0).getTupleOrdinal();
        this.columnReferences = requireNonNull(columnReferences);
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        // Extract grouped columns for provided ordinal
        Set<String> singleValueColumns = columnReferences.getOrDefault(ordinal, emptySet());
        // Return a collection tuple for the provided ordinal and group columns
        return new CollectionTuple(tuples, ordinal, singleValueColumns);
    }

    @Override
    public Object getValue(String column)
    {
        // Get all columns for all tuples
        return new TransformIterator(tuples.iterator(), tuple -> ((Tuple) tuple).getValue(column));
    }

    @Override
    public Iterator<TupleColumn> getColumns(int tupleOrdinal)
    {
        // TODO: Might need to distinct all grouped rows columns since there might be different ones further down
        // But for now just return the first rows columns
        return tuples.get(0).getColumns(tupleOrdinal);
    }
}
