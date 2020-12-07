package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Operator that takes the union of provided operators */
class UnionOperator extends AOperator
{
    private final Operator outer;
    private final Operator inner;
    private final boolean all;

    UnionOperator(int nodeId, Operator outer, Operator inner, boolean all)
    {
        super(nodeId);
        this.outer = outer;
        this.inner = inner;
        this.all = all;
        if (!all)
        {
            throw new IllegalArgumentException("Distinct UNION not implemrnted");
        }
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(outer, inner);
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        final RowIterator itO = outer.open(context);
        final RowIterator itI = inner.open(context);

        //CSOFF
        return new RowIterator()
        //CSON
        {
            private RowIterator current = itO;

            @Override
            public Tuple next()
            {
                return current.next();
            }

            @Override
            public boolean hasNext()
            {
                if (!current.hasNext())
                {
                    if (current == itI)
                    {
                        return false;
                    }
                    current = itI;
                }

                return current.hasNext();
            }

            @Override
            public void close()
            {
                itO.close();
                itI.close();
            }
        };
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + outer.hashCode();
        hashCode = hashCode * 37 + inner.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof UnionOperator)
        {
            UnionOperator that = (UnionOperator) obj;
            return nodeId == that.nodeId
                && outer.equals(that.outer)
                && inner.equals(that.inner)
                && all == that.all;
        }
        return false;
    }
}
