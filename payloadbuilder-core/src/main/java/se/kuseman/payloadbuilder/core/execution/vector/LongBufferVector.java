package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.LongBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link ValueVector} for longs backed with {@link LongBuffer} */
class LongBufferVector extends ABufferVector
{
    private final LongBuffer buffer;
    private final int bufferStartPosition;

    public LongBufferVector(LongBuffer buffer, int bufferStartPosition, int startPosition, int size, BitBuffer nullBuffer)
    {
        super(ResolvedType.of(Type.Long), size, nullBuffer, startPosition);
        this.buffer = buffer;
        this.bufferStartPosition = bufferStartPosition + startPosition;
    }

    @Override
    public long getLong(int row)
    {
        return buffer.get(bufferStartPosition + row);
    }
}
