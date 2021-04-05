package org.kuse.payloadbuilder.core.operator;

/** Merges outer and inner tuple */
class DefaultTupleMerger implements TupleMerger
{
    /** Limit number of merged rows */
    private final int limit;
    private final int innerTupleOrdinal;
    /**
     * Size of composite tuple to create. This is the size of the table aliases the same level
     */
    private final int compositeTupleCount;

    DefaultTupleMerger(int limit, int innerTupleOrdinal, int compositeTupleCount)
    {
        this.limit = limit;
        this.innerTupleOrdinal = innerTupleOrdinal;
        this.compositeTupleCount = compositeTupleCount;
    }

    @Override
    public Tuple merge(Tuple outer, Tuple inner, boolean populating)
    {
        // No populating merge, create/or merge a composite tuple
        if (!populating)
        {
            if (!(outer instanceof CompositeTuple))
            {
                return new CompositeTuple(outer, inner, compositeTupleCount);
            }

            CompositeTuple outerTuple = (CompositeTuple) outer;

            // Unwrap the inner tuples and add to outer to
            // get a flat structure
            if (inner instanceof CompositeTuple)
            {
                outerTuple.addAll((CompositeTuple) inner);
            }
            else
            {
                outerTuple.add(inner);
            }

            return outerTuple;
        }

        CompositeTuple outerTuple;
        CollectionTuple populatingTuple;
        if (!(outer instanceof CompositeTuple))
        {
            populatingTuple = new CollectionTuple(inner);
            return new CompositeTuple(outer, populatingTuple, compositeTupleCount);
        }

        outerTuple = (CompositeTuple) outer;

        // Fetch the populating tuple from the outer
        Tuple tuple = outerTuple.getTuple(innerTupleOrdinal);

        // No ordinal found in outer => first tuple of this ordinal
        // create a collection tuple and add it to outer
        if (tuple == null)
        {
            tuple = new CollectionTuple(innerTupleOrdinal);
            outerTuple.add(tuple);
        }
        else if (!(tuple instanceof CollectionTuple))
        {
            throw new RuntimeException("Expected a collection tuple but got: " + tuple);
        }

        // Append the inner to the collection
        ((CollectionTuple) tuple).add(inner);

        return outerTuple;
    }

    @Override
    public int hashCode()
    {
        return limit;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof DefaultTupleMerger)
        {
            DefaultTupleMerger that = (DefaultTupleMerger) obj;
            return limit == that.limit
                && innerTupleOrdinal == that.innerTupleOrdinal
                && compositeTupleCount == that.compositeTupleCount;
        }
        return false;
    }
}
