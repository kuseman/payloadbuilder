package org.kuse.payloadbuilder.core.operator;

/** Merges outer and inner tuple */
class DefaultTupleMerger implements TupleMerger
{
    /** Default merger. Merges inner row into outer */
    public static final DefaultTupleMerger DEFAULT = new DefaultTupleMerger(-1);

    /** Limit number of merged rows */
    private final int limit;

    DefaultTupleMerger(int limit)
    {
        this.limit = limit;
    }

    @Override
    public Tuple merge(Tuple outer, Tuple inner, boolean populating, int nodeId)
    {
        // No populating merge, create/or merge a composite tuple
        if (!populating)
        {
            // Outer is already a composite tuple
            // make a copy and add merge inner to get a flat structure
            if (outer instanceof CompositeTuple)
            {
                CompositeTuple tuple = (CompositeTuple) outer;
                CompositeTuple newTuple = new CompositeTuple(tuple);
                newTuple.add(inner);
                return newTuple;
            }

            return new CompositeTuple(outer, inner);
        }

        CompositeTuple outerTuple;
        CollectionTuple populatingTuple;
        if (!(outer instanceof CompositeTuple))
        {
            populatingTuple = new CollectionTuple(inner);
            return new CompositeTuple(outer, populatingTuple);
        }

        outerTuple = (CompositeTuple) outer;

        // Fetch the populating tuple from the outer
        Tuple tuple = outerTuple.getTuple(inner.getTupleOrdinal());

        if (!(tuple instanceof CollectionTuple))
        {
            throw new RuntimeException("Expected a populating tuple but got: " + tuple);
        }

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
            return limit == ((DefaultTupleMerger) obj).limit;
        }
        return false;
    }

    /** Create a limiting row merger */
    static DefaultTupleMerger limit(int limit)
    {
        return new DefaultTupleMerger(limit);
    }
}
