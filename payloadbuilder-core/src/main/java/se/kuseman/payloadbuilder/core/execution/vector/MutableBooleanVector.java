package se.kuseman.payloadbuilder.core.execution.vector;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A boolean mutable value vector */
class MutableBooleanVector extends AMutableVector
{
    private BitBuffer valueBuffer;

    MutableBooleanVector(VectorFactory factory, int estimatedCapacity)
    {
        super(factory, estimatedCapacity, ResolvedType.of(Type.Boolean));
    }

    @Override
    public void setBoolean(int row, boolean value)
    {
        size = Math.max(size, row + 1);
        ensureSize(row + 1);
        valueBuffer.put(row, value);
        removeNull(row);
    }

    @Override
    public boolean getBoolean(int row)
    {
        return valueBuffer.get(row);
    }

    @Override
    public void copy(int startRow, ValueVector source, int sourceRow, int length)
    {
        int newSize = startRow + length;
        ensureSize(newSize);
        size = Math.max(size, newSize);
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                valueBuffer.put(dr, source.getBoolean(sr));
            }
        }
    }

    private void ensureSize(int limit)
    {
        if (valueBuffer == null)
        {
            valueBuffer = factory.getAllocator()
                    .getBitBuffer(estimatedCapacity);
        }
    }
}
