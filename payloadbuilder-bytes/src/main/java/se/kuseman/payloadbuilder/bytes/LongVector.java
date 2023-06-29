package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Long vector */
class LongVector extends AVector
{
    static final byte VERSION = 1;

    LongVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.Long), size, nullBuffer, startPosition);
    }

    @Override
    public long getLong(int row)
    {
        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        return buffer.getLong(valueOffset);
    }

    /** Create long vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(LongVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalLong(buffer.getLong(valueOffset), size);
        }

        return new LongVector(buffer, position, nullBuffer, size);
    }
}
