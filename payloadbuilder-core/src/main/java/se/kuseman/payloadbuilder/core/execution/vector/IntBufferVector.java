package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.IntBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link ValueVector} for ints backed with {@link IntBuffer} */
class IntBufferVector extends ABufferVector
{
    private final IntBuffer buffer;
    private final int bufferStartPosition;

    public IntBufferVector(IntBuffer buffer, int bufferStartPosition, int startPosition, int size, BitBuffer nullBuffer)
    {
        super(ResolvedType.of(Type.Int), size, nullBuffer, startPosition);
        this.buffer = buffer;
        this.bufferStartPosition = bufferStartPosition + startPosition;
    }

    @Override
    public int getInt(int row)
    {
        return buffer.get(bufferStartPosition + row);
    }
}
