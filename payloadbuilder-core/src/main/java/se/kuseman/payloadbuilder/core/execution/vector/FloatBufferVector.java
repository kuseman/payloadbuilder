package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.FloatBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link ValueVector} for longs backed with {@link FloatBuffer} */
class FloatBufferVector extends ABufferVector
{
    private final FloatBuffer buffer;
    private final int bufferStartPosition;

    public FloatBufferVector(FloatBuffer buffer, int bufferStartPosition, int startPosition, int size, BitBuffer nullBuffer)
    {
        super(ResolvedType.of(Type.Float), size, nullBuffer, startPosition);
        this.buffer = buffer;
        this.bufferStartPosition = bufferStartPosition + startPosition;
    }

    @Override
    public float getFloat(int row)
    {
        return buffer.get(bufferStartPosition + row);
    }
}
