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

    public Tuple getContextOuter()
    {
        return contextOuter;
    }

    public Tuple getOuter()
    {
        return outer;
    }

    public void setOuter(Tuple outer)
    {
        this.outer = outer;
    }

    public Tuple getInner()
    {
        return inner;
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
        throw new RuntimeException("Join tuple does not have a tuple orinal");
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        /*
         * tableA                   (to = 0, contextOuter)
         *   tableB                 (to = 1)
         *   tableC                 (to = 2, outer)
         *     tabbleD              (to = 3)
         *   tableE                 (to = 4, inner)
         */

        int innerTupleOrdinal = inner.getTupleOrdinal();

        // A tuple higher up in hierarchy is wanted
        if (contextOuter != null && ordinal <= contextOuter.getTupleOrdinal())
        {
            return contextOuter.getTuple(ordinal);
        }
        else if (outer != null)
        {
            if (ordinal <= outer.getTupleOrdinal() || ordinal < innerTupleOrdinal)
            {
                return outer.getTuple(ordinal);
            }
        }

        return inner.getTuple(ordinal);
    }

    @Override
    public int getColumnCount()
    {
        return inner.getColumnCount();
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return inner.getColumnOrdinal(column);
    }

    @Override
    public String getColumn(int ordinal)
    {
        return inner.getColumn(ordinal);
    }

    @Override
    public Object getValue(int ordinal)
    {
        // This is a qualifier with a single part then it can only reference
        // the inner
        return inner.getValue(ordinal);
    }
}
