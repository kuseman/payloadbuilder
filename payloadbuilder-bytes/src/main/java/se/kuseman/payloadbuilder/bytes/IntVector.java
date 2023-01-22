package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Int vector */
class IntVector extends AVector
{
    static final byte VERSION = 1;

    IntVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.Int), size, nullBuffer, startPosition);
    }

    @Override
    public int getInt(int row)
    {
        int offset = dataStartPosition + (row * Integer.BYTES);
        return buffer.getInt(offset);
    }

    /** Create int vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(IntVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            int value = buffer.getInt(position);
            return ValueVector.literalInt(value, size);
        }

        return new IntVector(buffer, position, nullBuffer, size);
    }
}
