package se.kuseman.payloadbuilder.core.execution.vector;

import java.util.BitSet;

/** Simple bit buffer currently implemented with {@link BitSet} */
class BitBuffer
{
    private final BitSet bitSet;

    BitBuffer(int estimatedSize)
    {
        this.bitSet = new BitSet(estimatedSize);
    }

    void put(int index, boolean value)
    {
        bitSet.set(index, value);
    }

    void put(int fromIndex, int toIndex, boolean value)
    {
        bitSet.set(fromIndex, toIndex, value);
    }

    boolean get(int index)
    {
        return bitSet.get(index);
    }

    void clear()
    {
        bitSet.clear();
    }

    @Override
    public String toString()
    {
        return bitSet.toString();
    }

    int getCardinality()
    {
        return bitSet.cardinality();
    }
}
