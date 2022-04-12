package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Merges outer and inner tuple */
class DefaultTupleMerger implements TupleMerger
{
    /** Limit number of merged rows */
    private final int limit;
    private final int innerTupleOrdinal;
    /**
     * <pre>
     * Size of composite tuple to create. This is the size of the table aliases the same level including the
     * the outer table source
     * </pre>
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
            return outerTuple.add(inner);
        }

        CompositeTuple outerTuple;
        CollectionTuple populatingTuple;
        if (!(outer instanceof CompositeTuple))
        {
            populatingTuple = new CollectionTuple(inner, innerTupleOrdinal);
            return new CompositeTuple(outer, populatingTuple, compositeTupleCount);
        }

        outerTuple = (CompositeTuple) outer;

        // Fetch the populating tuple from the outer
        Tuple tuple = outerTuple.getCollectionTupleForMerge(innerTupleOrdinal);

        // No ordinal found in outer => first tuple of this ordinal
        // create a collection tuple and add it to outer
        if (tuple == null)
        {
            tuple = new CollectionTuple(inner, innerTupleOrdinal);
            outerTuple.add(tuple);
            return outerTuple;
        }
        else if (!(tuple instanceof CollectionTuple))
        {
            throw new RuntimeException("Expected a collection tuple but got: " + tuple);
        }

        // Append the inner to the collection
        ((CollectionTuple) tuple).addTuple(inner);

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

    @Override
    public String toString()
    {
        return String.format("LIMIT: %d, INNER TUPLE ORDINAL: %d, TUPLE COUNT: %d", limit, innerTupleOrdinal, compositeTupleCount);
    }
}
