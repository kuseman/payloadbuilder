package org.kuse.payloadbuilder.core.operator;

/** Tuple used during join that connects two tuples and checks match */
class JoinTuple implements Tuple
{
    /** Tuple used when having a correlated nested loop, and points to the current outer tuple */
    private final Tuple contextOuter;
    private Tuple outer;
    private Tuple inner;

    JoinTuple(Tuple contextOuter)
    {
        this.contextOuter = contextOuter;
    }

    public void setOuter(Tuple outer)
    {
        this.outer = outer;
    }

    public void setInner(Tuple inner)
    {
        this.inner = inner;
    }

    @Override
    public int getTupleOrdinal()
    {
        // This is a root tuple that should never be compared to a tuple ordinal
        // it's only used during a join process
        throw new IllegalArgumentException("Not implemented");
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        if (inner == null)
        {
            return -1;
        }
        return inner.getColumnOrdinal(column);
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        if (inner == null)
        {
            return null;
        }
        return inner.getValue(columnOrdinal);
    }

    @Override
    public boolean isNull(int columnOrdinal)
    {
        throw new RuntimeException("Implement");
    }

    @Override
    public Tuple getTuple(int tupleOrdinal)
    {
        /*
        * tableA                   (to = 0, contextOuter)
        *   tableB                 (to = 1)
        *   tableC                 (to = 2, outer)
        *     tabbleD              (to = 3)
        *   tableE                 (to = 4, inner)
        */

        // Closest tuple is the inner
        if (tupleOrdinal == -1)
        {
            return inner != null ? inner.getTuple(tupleOrdinal) : null;
        }

        // A tuple higher up in hierarchy is wanted
        if (contextOuter != null
            && tupleOrdinal <= contextOuter.getTupleOrdinal())
        {
            return contextOuter.getTuple(tupleOrdinal);
        }
        else if (outer != null
            && (tupleOrdinal <= outer.getTupleOrdinal()
                || tupleOrdinal < inner.getTupleOrdinal()))
        {
            return outer.getTuple(tupleOrdinal);
        }

        return inner.getTuple(tupleOrdinal);
    }
}
