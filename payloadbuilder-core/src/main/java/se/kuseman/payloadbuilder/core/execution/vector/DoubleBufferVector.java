package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.DoubleBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link ValueVector} for longs backed with {@link DoubleBuffer} */
class DoubleBufferVector extends ABufferVector
{
    private final DoubleBuffer buffer;
    private final int bufferStartPosition;

    public DoubleBufferVector(DoubleBuffer buffer, int bufferStartPosition, int startPosition, int size, BitBuffer nullBuffer)
    {
        super(ResolvedType.of(Type.Double), size, nullBuffer, startPosition);
        this.buffer = buffer;
        this.bufferStartPosition = bufferStartPosition + startPosition;
    }

    @Override
    public double getDouble(int row)
    {
        return buffer.get(bufferStartPosition + row);
    }
}
