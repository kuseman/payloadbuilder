package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleList;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/**
 * A tuple iterator that wraps another tuple iterator and takes care of detecting if the wrapped is a TupleList or not
 */
public class ATupleIterator implements TupleIterator
{
    private final TupleIterator wrapped;
    private final TupleList list;
    private int count;
    private Tuple next;

    public ATupleIterator(TupleIterator wrapped)
    {
        this.wrapped = wrapped;
        this.list = wrapped instanceof TupleList ? (TupleList) wrapped
                : null;
    }

    @Override
    public boolean hasNext()
    {
        return setNextInternal();
    }

    @Override
    public Tuple next()
    {
        if (next == null)
        {
            throw new IllegalStateException("No more elements");
        }

        Tuple result = process(next);
        next = null;
        return result;
    }

    @Override
    public void close()
    {
        wrapped.close();
    }

    /**
     * Returns true if provided tuple should be the next tuple or not Can be used for filtering purposes
     *
     * @param tuple Next tuple to be returned or not
     */
    protected boolean setNext(Tuple tuple)
    {
        return true;
    }

    /** Process current tuple */
    protected Tuple process(Tuple tuple)
    {
        return tuple;
    }

    private boolean setNextInternal()
    {
        while (next == null)
        {
            if (list != null
                    && count >= list.size())
            {
                return false;
            }
            else if (list == null
                    && !wrapped.hasNext())
            {
                return false;
            }

            Tuple tuple = list == null ? wrapped.next()
                    : list.get(count);
            count++;
            if (setNext(tuple))
            {
                next = tuple;
            }
        }

        return true;
    }
}
