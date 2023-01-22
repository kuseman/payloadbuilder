package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.LongBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.ILongVectorBuilder;

/** Builder for {@link Column.Type#Long} vectors. */
class LongBufferVectorBuilder extends ABufferVectorBuilder implements ILongVectorBuilder
{
    private LongBuffer buffer;

    private boolean first = true;
    private int literalSize;
    private long prevValue;
    private boolean prevIsNull;
    private int appendingLength;

    LongBufferVectorBuilder(BufferAllocator allocator, int estimatedSize)
    {
        super(allocator, estimatedSize);
    }

    @Override
    void ensureSize(int appendingLength)
    {
        if (buffer != null)
        {
            int neededLimit = buffer.position() + appendingLength;
            buffer = ensureSizeOfBuffer(buffer, neededLimit, c -> allocator.getLongBuffer(c), (t, tn) -> tn.put(t));
        }
        else
        {
            this.appendingLength = appendingLength;
        }
    }

    @Override
    public void put(long value)
    {
        if (buffer != null)
        {
            ensureSize(1);
        }
        append(value, false);
        nullBufferOffset++;
        size++;
    }

    @Override
    void put(boolean isNull, ValueVector source, int sourceRow, int count)
    {
        long value = isNull ? 0
                : source.getLong(sourceRow);
        for (int i = 0; i < count; i++)
        {
            append(value, isNull);
        }
    }

    @Override
    public ValueVector build()
    {
        if (buffer == null)
        {
            return nullBuffer != null ? ValueVector.literalNull(ResolvedType.of(Type.Long), size)
                    : ValueVector.literalLong(prevValue, size);
        }
        return new LongBufferVector(buffer, bufferStartPosition, 0, size, nullBuffer);
    }

    private void append(long value, boolean isNull)
    {
        if (buffer == null)
        {
            if (first)
            {
                prevValue = value;
                prevIsNull = isNull;
                first = false;
            }
            // Switch to buffer
            else if (prevValue != value
                    || prevIsNull != isNull)
            {
                buffer = allocator.getLongBuffer(estimatedSize);
                bufferStartPosition = buffer.position();
                ensureSize(literalSize + 1 + appendingLength);
                for (int i = 0; i < literalSize; i++)
                {
                    buffer.put(prevValue);
                }
                buffer.put(value);
            }
            literalSize++;
        }
        else
        {
            buffer.put(value);
        }
    }
}
