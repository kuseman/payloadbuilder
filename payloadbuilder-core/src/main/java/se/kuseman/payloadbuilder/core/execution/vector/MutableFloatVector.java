package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.FloatBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A mutable float value vector */
class MutableFloatVector extends AMutableVector
{
    private FloatBuffer buffer;

    MutableFloatVector(VectorFactory factory, int estimatedCapacity)
    {
        super(factory, estimatedCapacity, ResolvedType.of(Type.Float));
    }

    @Override
    public void setFloat(int row, float value)
    {
        size = Math.max(size, row + 1);
        ensureSize(row + 1);
        buffer.put(row, value);
        removeNull(row);
    }

    @Override
    public float getFloat(int row)
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
                buffer.put(dr, source.getFloat(sr));
            }
        }
    }

    private void ensureSize(int limit)
    {
        if (buffer == null)
        {
            buffer = factory.getAllocator()
                    .getFloatBuffer(Math.max(estimatedCapacity, limit));
        }
        else if (buffer.limit() < limit)
        {
            FloatBuffer newBuffer = factory.getAllocator()
                    .getFloatBuffer(Math.max(estimatedCapacity, limit * 2));
            newBuffer.put(buffer);
            newBuffer.position(0);
            buffer = newBuffer;
        }
    }
}
