package se.kuseman.payloadbuilder.bytes;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Base class for vectors */
abstract class AVector implements ValueVector
{
    /** Size of a reference inside vectors */
    static final int REFERENCE_HEADER_SIZE = Integer.BYTES;

    protected final ByteBuffer buffer;
    protected final ResolvedType type;
    private final int length;
    private final NullBuffer nullBuffer;
    protected int dataStartPosition;

    AVector(ByteBuffer buffer, ResolvedType type, int length, NullBuffer nullBuffer, int dataStartPosition)
    {
        this.buffer = requireNonNull(buffer, "buffer");
        this.type = requireNonNull(type, "type");
        this.length = length;
        this.nullBuffer = requireNonNull(nullBuffer, "nullBuffer");
        this.dataStartPosition = dataStartPosition;
    }

    @Override
    public ResolvedType type()
    {
        return type;
    }

    @Override
    public int size()
    {
        return length;
    }

    @Override
    public boolean isNull(int row)
    {
        return nullBuffer.isNull(row);
    }

    static void throwUnknownVersion(Class<?> clazz, byte version)
    {
        throw new IllegalArgumentException("Unknown version: " + version + " for " + clazz.getSimpleName());
    }
}
