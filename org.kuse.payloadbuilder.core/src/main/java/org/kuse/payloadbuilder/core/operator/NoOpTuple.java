package org.kuse.payloadbuilder.core.operator;

/** No op tuple used for selects with no table source */
public class NoOpTuple implements Tuple
{
    public static final Tuple NO_OP = new NoOpTuple();

    @Override
    public int getTupleOrdinal()
    {
        return -1;
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return -1;
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        return null;
    }
}