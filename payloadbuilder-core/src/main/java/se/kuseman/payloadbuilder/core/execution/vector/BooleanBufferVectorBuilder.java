package se.kuseman.payloadbuilder.core.execution.vector;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IBooleanVectorBuilder;

/** Builder for {@link Column.Type#Int} vectors. */
class BooleanBufferVectorBuilder extends ABufferVectorBuilder implements IBooleanVectorBuilder
{
    private BitBuffer buffer;
    private int bufferPosition;

    BooleanBufferVectorBuilder(BufferAllocator allocator, int estimatedSize)
    {
        super(allocator, estimatedSize);
        this.buffer = allocator.getBitBuffer(estimatedSize);
    }

    @Override
    void ensureSize(int appendingLength)
    {
    }

    @Override
    public void put(boolean value)
    {
        buffer.put(bufferPosition, value);
        nullBufferOffset++;
        size++;
        bufferPosition++;
    }

    @Override
    void put(boolean isNull, ValueVector source, int sourceRow, int count)
    {
        boolean value = isNull ? false
                : source.getBoolean(sourceRow);
        for (int i = 0; i < count; i++)
        {
            buffer.put(bufferPosition + i, value);
        }
        bufferPosition += count;
    }

    @Override
    public ValueVector build()
    {
        return new BooleanBufferVector(buffer, 0, size, nullBuffer);
    }
}
