package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.DoubleBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A mutable double value vector */
class MutableDoubleVector extends AMutableVector
{
    private DoubleBuffer buffer;

    MutableDoubleVector(VectorFactory factory, int estimatedCapacity)
    {
        super(factory, estimatedCapacity, ResolvedType.of(Type.Double));
    }

    @Override
    public void setDouble(int row, double value)
    {
        size = Math.max(size, row + 1);
        ensureSize(row + 1);
        buffer.put(row, value);
        removeNull(row);
    }

    @Override
    public double getDouble(int row)
    {
        return buffer.get(row);
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
                buffer.put(dr, source.getDouble(sr));
            }
        }
    }

    private void ensureSize(int limit)
    {
        if (buffer == null)
        {
            buffer = factory.getAllocator()
                    .getDoubleBuffer(Math.max(estimatedCapacity, limit));
        }
        else if (buffer.limit() < limit)
        {
            DoubleBuffer newBuffer = factory.getAllocator()
                    .getDoubleBuffer(Math.max(estimatedCapacity, limit));
            newBuffer.put(buffer);
            newBuffer.position(0);
            buffer = newBuffer;
        }
    }
}
