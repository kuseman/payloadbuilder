package org.kuse.payloadbuilder.core.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.iterators.TransformIterator;

/**
 * Tuple that is composed of tuples of the same table source in a collection. type fashion. Used by populating joins and group by rows
 **/
class CollectionTuple extends ArrayList<Tuple> implements Tuple
{
    private final int tupleOrdinal;
    private final Set<Integer> singleValueOrdinals;

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
    CollectionTuple(List<Tuple> tuples, int targetOrdinal, Set<Integer> singleValueOrdinals)
    {
        super(tuples.size());
        addAll(tuples);
        this.targetOrdinal = targetOrdinal;
        this.tupleOrdinal = targetOrdinal;
        this.singleValueOrdinals = singleValueOrdinals;
    }

    /** Create a collection tuple based on another tuple */
    CollectionTuple(Tuple tuple)
    {
        add(tuple);
        this.tupleOrdinal = tuple.getTupleOrdinal();
        this.targetOrdinal = -1;
        this.singleValueOrdinals = null;
    }

    /** Create a collection tuple based on a tuple ordinal */
    CollectionTuple(int tupleOrdinal)
    {
        this.tupleOrdinal = tupleOrdinal;
        this.targetOrdinal = -1;
        this.singleValueOrdinals = null;
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        return get(0).getTuple(ordinal);
    }

    @Override
    public int getColumnCount()
    {
        // Use first tuple
        Tuple tuple = get(0);

        // Adapt for grouped row
        if (targetOrdinal != -1 && tuple.getTupleOrdinal() != targetOrdinal)
        {
            tuple = tuple.getTuple(targetOrdinal);
        }

        return tuple.getColumnCount();
    }

    @Override
    public String getColumn(int ordinal)
    {
        // Use first tuple
        Tuple tuple = get(0);

        // Adapt for grouped row
        if (targetOrdinal != -1 && tuple.getTupleOrdinal() != targetOrdinal)
        {
            tuple = tuple.getTuple(targetOrdinal);
        }

        return tuple.getColumn(ordinal);
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        // Use first tuple
        Tuple tuple = get(0);

        // Adapt for grouped row
        if (targetOrdinal != -1 && tuple.getTupleOrdinal() != targetOrdinal)
        {
            tuple = tuple.getTuple(targetOrdinal);
        }

        return tuple.getColumnOrdinal(column);
    }

    @Override
    public Object getValue(int ordinal)
    {
        if (singleValueOrdinals != null)
        {
            if (singleValueOrdinals.contains(ordinal))
            {
                Tuple sub = get(0).getTuple(targetOrdinal);
                return sub != null ? sub.getValue(ordinal) : null;
            }
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
}
