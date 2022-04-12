package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Test tuple */
public class TestTuple implements Tuple
{
    private final int tupleOrdinal;
    boolean optimized;

    public TestTuple(int tupleOrdinal)
    {
        this.tupleOrdinal = tupleOrdinal;
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public int getColumnCount()
    {
        return this.tupleOrdinal;
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return this.tupleOrdinal;
    }

    @Override
    public String getColumn(int columnOrdinal)
    {
        return "c" + columnOrdinal + "_" + this.tupleOrdinal;
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        return "v" + columnOrdinal + "_" + this.tupleOrdinal;
    }

    @Override
    public String toString()
    {
        return "Ord: " + tupleOrdinal;
    }
}