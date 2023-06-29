package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Double vector */
class DoubleVector extends AVector
{
    static final byte VERSION = 1;

    DoubleVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.Double), size, nullBuffer, startPosition);
    }

    @Override
    public double getDouble(int row)
    {
        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        return buffer.getDouble(valueOffset);
    }

    /** Create long vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(DoubleVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalDouble(buffer.getDouble(valueOffset), size);
        }

        return new DoubleVector(buffer, position, nullBuffer, size);
    }
}
