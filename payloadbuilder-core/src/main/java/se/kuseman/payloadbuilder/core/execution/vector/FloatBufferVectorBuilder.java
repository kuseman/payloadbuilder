package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.FloatBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IFloatVectorBuilder;

/** Builder for {@link Column.Type#Float} vectors. */
class FloatBufferVectorBuilder extends ABufferVectorBuilder implements IFloatVectorBuilder
{
    private FloatBuffer buffer;

    private boolean first = true;
    private int literalSize;
    private float prevValue;
    private boolean prevIsNull;
    private int appendingLength;

    FloatBufferVectorBuilder(BufferAllocator allocator, int estimatedSize)
    {
        super(allocator, estimatedSize);
    }

    @Override
    void ensureSize(int appendingLength)
    {
        if (buffer != null)
        {
            int neededLimit = buffer.position() + appendingLength;
            buffer = ensureSizeOfBuffer(buffer, neededLimit, c -> allocator.getFloatBuffer(c), (t, tn) -> tn.put(t));
        }
        else
        {
            this.appendingLength = appendingLength;
        }
    }

    @Override
    public void put(float value)
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
        float value = isNull ? 0
                : source.getFloat(sourceRow);

        for (int j = 0; j < count; j++)
        {
            append(value, isNull);
        }
    }

    @Override
    public ValueVector build()
    {
        if (buffer == null)
        {
            return nullBuffer != null ? ValueVector.literalNull(ResolvedType.of(Type.Float), size)
                    : ValueVector.literalFloat(prevValue, size);
        }
        return new FloatBufferVector(buffer, bufferStartPosition, 0, size, nullBuffer);
    }

    private void append(float value, boolean isNull)
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
                buffer = allocator.getFloatBuffer(estimatedSize);
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
