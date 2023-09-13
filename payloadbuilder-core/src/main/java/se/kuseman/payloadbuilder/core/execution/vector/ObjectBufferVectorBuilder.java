package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;

/** Builders that uses Java object list/arrays for buffer */
class ObjectBufferVectorBuilder extends ABufferVectorBuilder implements IObjectVectorBuilder
{
    private final ResolvedType type;
    private final Class<?> expectedType;
    private final ValueExtractor extractor;
    private final LiteralCreator literalCreator;
    private List<Object> buffer;
    // TODO: Dictionary storage to allow for skipping duplicated values

    /**
     * Value to use to detect if we can create a literal value instead of a buffer. If all values was equal during build we can return a literal instead of a buffer vector
     */
    private Object currentValue;
    private boolean first = true;
    private int literalSize;

    ObjectBufferVectorBuilder(BufferAllocator allocator, int estimatedSize, ResolvedType type, Class<?> expectedType, ValueExtractor extractor, LiteralCreator literalCreator)
    {
        super(allocator, estimatedSize);
        this.type = type;
        this.expectedType = requireNonNull(expectedType);
        this.extractor = requireNonNull(extractor);
        this.literalCreator = requireNonNull(literalCreator);
    }

    /** Ensure size of buffer for provided length */
    @Override
    void ensureSize(int appendingLength)
    {
    }

    /** Object buffer can store nulls in the actual buffer so no need for these overloads */
    @Override
    protected void putNulls(boolean isNull, int replicationCount)
    {
    }

    @Override
    public void putNull()
    {
        put(null);
    }

    @Override
    public void put(Object value)
    {
        if (value != null
                && !expectedType.isAssignableFrom(value.getClass()))
        {
            throw new IllegalArgumentException("Expected a " + type.getType() + " value but got: " + value);
        }

        append(value);
        size++;
    }

    /** Put value in current position of buffer */
    @Override
    void put(boolean isNull, ValueVector source, int sourceRow, int count)
    {
        Object value = isNull ? null
                : extractor.extract(source, sourceRow);
        for (int i = 0; i < count; i++)
        {
            append(value);
        }
    }

    @Override
    public ValueVector build()
    {
        // Return a literal vector
        if (buffer == null)
        {
            return currentValue == null ? ValueVector.literalNull(type, size)
                    : literalCreator.create(currentValue, type, size);
        }

        // TODO: If type = Any this can be converted into a primitive buffer
        return new ObjectBufferVector(buffer, type, 0, size);
    }

    private void append(Object value)
    {
        if (buffer == null)
        {
            if (first)
            {
                currentValue = value;
                first = false;
            }
            // Switch to buffer
            else if (!Objects.equals(currentValue, value))
            {
                buffer = allocator.getObjectBuffer(estimatedSize);
                // Add all current values in buffer
                for (int i = 0; i < literalSize; i++)
                {
                    buffer.add(currentValue);
                }
                buffer.add(value);
            }
            literalSize++;
        }
        else
        {
            buffer.add(value);
        }
    }

    @FunctionalInterface
    interface ValueExtractor
    {
        Object extract(ValueVector vector, int row);
    }

    @FunctionalInterface
    interface LiteralCreator
    {
        ValueVector create(Object value, ResolvedType type, int size);
    }
}
