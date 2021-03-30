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
         *
         */

        // In joins we're always work at the bottom of the tree
        // so no need to traverse the inner
        if (ordinal > inner.getTupleOrdinal())
        {
            return inner.getTuple(ordinal);
        }
        else if (ordinal == inner.getTupleOrdinal())
        {
            return inner;
        }
        else if (outer != null)
        {
            if (ordinal == outer.getTupleOrdinal())
            {
                return outer;
            }
            else if (ordinal > outer.getTupleOrdinal())
            {
                return outer.getTuple(ordinal);
            }
        }

        if (contextOuter != null)
        {
            if (ordinal == contextOuter.getTupleOrdinal())
            {
                return contextOuter;
            }

            return contextOuter.getTuple(ordinal);
        }

        return null;
    }

    @Override
    public Object getValue(int ordinal)
    {
        // This is a qualifier with a single part then it can only reference
        // the inner
        return inner.getValue(ordinal);
    }

    @Override
    public Object getValue(String column)
    {
        // This is a qualifier with a single part then it can only reference
        // the inner
        return inner.getValue(column);
    }
}
