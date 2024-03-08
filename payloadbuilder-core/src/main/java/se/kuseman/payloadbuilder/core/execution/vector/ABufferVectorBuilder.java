package se.kuseman.payloadbuilder.core.execution.vector;

import java.nio.Buffer;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;

/** Base class for value vector builders */
abstract class ABufferVectorBuilder implements IValueVectorBuilder
{
    protected final BufferAllocator allocator;
    protected final int estimatedSize;
    protected int size = 0;
    protected BitBuffer nullBuffer;
    protected int nullBufferOffset = 0;

    /** Value used in implementations working with {@link Buffer}'s to keep track of where it's buffer starts */
    protected int bufferStartPosition;

    ABufferVectorBuilder(BufferAllocator allocator, int estimatedSize)
    {
        this.allocator = allocator;
        this.estimatedSize = estimatedSize;
    }

    /** Ensure size of buffer for provided length */
    abstract void ensureSize(int appendingLength);

    @Override
    public void copy(ValueVector source)
    {
        int sourceSize = source.size();
        ensureSize(sourceSize);
        for (int i = 0; i < sourceSize; i++)
        {
            boolean isNull = source.isNull(i);
            put(isNull, source, i, 1);
            putNulls(isNull, 1);
        }
        size += sourceSize;
    }

    @Override
    public void putNull()
    {
        ensureSize(1);
        put(true, null, -1, 1);
        putNulls(true, 1);
        size++;
    }

    @Override
    public void put(ValueVector source, int sourceRow)
    {
        ensureSize(1);
        boolean isNull = source.isNull(sourceRow);
        put(isNull, source, sourceRow, 1);
        putNulls(isNull, 1);
        size++;
    }

    /** Put value in current position of buffer. Increase of size should NOT be handled by this method, it's the caller responsibility */
    abstract void put(boolean isNull, ValueVector source, int sourceRow, int count);

    /** Return a value vector from this builder */
    @Override
    public abstract ValueVector build();

    @Override
    public int size()
    {
        return size;
    }

    /** Put null is null buffer with replication count */
    protected void putNulls(boolean isNull, int replicationCount)
    {
        if (isNull)
        {
            if (nullBuffer == null)
            {
                nullBuffer = allocator.getBitBuffer(estimatedSize);
            }
            nullBuffer.put(nullBufferOffset, nullBufferOffset + replicationCount, true);
        }
        nullBufferOffset += replicationCount;
    }

    /** Resize provided buffer if needed */
    protected <T extends Buffer> T ensureSizeOfBuffer(T buffer, int neededLimit, IntFunction<T> bufferCreator, BiConsumer<T, T> bufferCopier)
    {
        T result = buffer;
        // Resize buffer if needed
        if (buffer.limit() < neededLimit)
        {
            // Create new buffer and store old positions (subtract the start position)
            result = bufferCreator.apply(neededLimit - bufferStartPosition);
            int oldPosition = buffer.position() - bufferStartPosition;
            int newStartPosition = result.position();

            // Transfer old data to new buffer
            buffer.position(bufferStartPosition);
            bufferCopier.accept(buffer, result);

            // Restore positions
            result.position(newStartPosition + oldPosition);
            bufferStartPosition = newStartPosition;
        }
        return result;
    }

    /** Create a {@link ABufferVectorBuilder} from provided type and size */
    static ABufferVectorBuilder getBuilder(ResolvedType type, BufferAllocator allocator, int estimatedSize)
    {
        // CSOFF
        switch (type.getType())
        // CSON
        {
            case Boolean:
                return new BooleanBufferVectorBuilder(allocator, estimatedSize);
            case Int:
                return new IntBufferVectorBuilder(allocator, estimatedSize);
            case Long:
                return new LongBufferVectorBuilder(allocator, estimatedSize);
            case Float:
                return new FloatBufferVectorBuilder(allocator, estimatedSize);
            case Double:
                return new DoubleBufferVectorBuilder(allocator, estimatedSize);
            case Any:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, Object.class, (v, r) -> v.getAny(r), (v, t, s) -> ValueVector.literalAny(s, v));
            case Array:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, ValueVector.class, (v, r) -> v.getArray(r), (v, t, s) -> ValueVector.literalArray((ValueVector) v, t, s));
            case DateTime:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, EpochDateTime.class, (v, r) -> v.getDateTime(r), (v, t, s) -> ValueVector.literalDateTime((EpochDateTime) v, s));
            case DateTimeOffset:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, EpochDateTimeOffset.class, (v, r) -> v.getDateTimeOffset(r),
                        (v, t, s) -> ValueVector.literalDateTimeOffset((EpochDateTimeOffset) v, s));
            case Decimal:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, Decimal.class, (v, r) -> v.getDecimal(r), (v, t, s) -> ValueVector.literalDecimal((Decimal) v, s));
            case Object:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, ObjectVector.class, (v, r) -> v.getObject(r), (v, t, s) -> ValueVector.literalObject((ObjectVector) v, t, s));
            case String:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, UTF8String.class, (v, r) -> v.getString(r), (v, t, s) -> ValueVector.literalString((UTF8String) v, s));
            case Table:
                return new ObjectBufferVectorBuilder(allocator, estimatedSize, type, TupleVector.class, (v, r) -> v.getTable(r), (v, t, s) -> ValueVector.literalTable((TupleVector) v, t, s));
            // No default case here!!!
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
