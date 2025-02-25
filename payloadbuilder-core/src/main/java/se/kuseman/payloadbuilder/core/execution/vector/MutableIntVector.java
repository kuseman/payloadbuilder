package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.IntBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A mutable int value vector */
class MutableIntVector extends AMutableVector
{
    private IntBuffer buffer;

    MutableIntVector(VectorFactory factory, int estimatedCapacity)
    {
        super(factory, estimatedCapacity, ResolvedType.of(Type.Int));
    }

    @Override
    public void setInt(int row, int value)
    {
        size = Math.max(size, row + 1);
        ensureSize(row + 1);
        buffer.put(row, value);
        removeNull(row);
    }

    @Override
    public int getInt(int row)
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
                buffer.put(dr, source.getInt(sr));
            }
        }
    }

    private void ensureSize(int limit)
    {
        if (buffer == null)
        {
            buffer = factory.getAllocator()
                    .getIntBuffer(Math.max(estimatedCapacity, limit));
        }
        else if (buffer.limit() < limit)
        {
            // We double the limit to avoid to much allocations
            // For example if we fetch a lot of rows from a source with batch size 500
            // we are going to resize and copy huge amount of times
            // This can however cause OOM's if handling really huge amount of rows
            IntBuffer newBuffer = factory.getAllocator()
                    .getIntBuffer(Math.max(estimatedCapacity, limit * 2));
            newBuffer.put(buffer);
            newBuffer.position(0);
            buffer = newBuffer;
        }
    }
}
