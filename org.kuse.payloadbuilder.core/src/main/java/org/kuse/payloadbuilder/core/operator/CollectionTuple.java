package org.kuse.payloadbuilder.core.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.iterators.TransformIterator;

/**
 * Tuple that is composed of tuples of the same table source in a collection. type fashion. Used by populating joins and group by rows
 **/
class CollectionTuple extends ArrayList<Tuple> implements Tuple
{
    private final int tupleOrdinal;
    private final Set<String> singleValueColumns;

    /**
     * Target ordinal used by grouped rows. This to know which ordinal to stream values from when returning values
     */
    private final int targetOrdinal;

    /**
     * Constructor used when this tuple is used as a grouped row tuple
     *
     * <pre>
     * If one of the provided single value columns is wanted then delegate to first
     * tuple in collection (they are all the same) else return an iterator of all tuples values
     * </pre>
     */
    CollectionTuple(List<Tuple> tuples, int targetOrdinal, Set<String> singleValueColumns)
    {
        this.targetOrdinal = targetOrdinal;
        addAll(tuples);
        this.tupleOrdinal = tuples.get(0).getTupleOrdinal();
        this.singleValueColumns = singleValueColumns;
    }

    CollectionTuple(Tuple tuple)
    {
        add(tuple);
        this.tupleOrdinal = tuple.getTupleOrdinal();
        this.targetOrdinal = -1;
        this.singleValueColumns = null;
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        // Populating is a collection of the same tuples
        // if delegate to first child
        // this is a typical accces to first row in a populated join
        return get(0).getTuple(ordinal);
    }

    @Override
    public Object getValue(int ordinal)
    {
        if (targetOrdinal >= 0)
        {
            // TODO: singleValueIndices
            // Stream values for target ordinal
            return new TransformIterator(iterator(), tuple ->
            {
                Tuple sub = ((Tuple) tuple).getTuple(targetOrdinal);
                return sub != null ? sub.getValue(ordinal) : null;
            });
        }

        // Delegate to first row
        return get(0).getValue(ordinal);
    }

    @Override
    public Object getValue(String column)
    {
        if (targetOrdinal >= 0)
        {
            // A grouped column, return first rows value, they are all the same
            if (singleValueColumns.contains(column))
            {
                Tuple sub = get(0).getTuple(targetOrdinal);
                return sub != null ? sub.getValue(column) : null;
            }

            // Stream all values for target ordinal
            return new TransformIterator(iterator(), tuple ->
            {
                Tuple sub = ((Tuple) tuple).getTuple(targetOrdinal);
                return sub != null ? sub.getValue(column) : null;
            });
        }

        // Delegate to first row
        return get(0).getValue(column);
    }

    @Override
    public Iterator<TupleColumn> getColumns(int tupleOrdinal)
    {
        // Delegate to first row
        return get(0).getColumns(tupleOrdinal);
    }
}
