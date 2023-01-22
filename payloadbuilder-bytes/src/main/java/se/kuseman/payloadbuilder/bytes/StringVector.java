package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** String vector */
class StringVector extends AVector
{
    static final byte VERSION = 1;

    StringVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.String), size, nullBuffer, startPosition);
    }

    @Override
    public UTF8String getString(int row)
    {
        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        return getString(buffer, valueOffset);
    }

    /** Create string vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(StringVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalString(getString(buffer, valueOffset), size);
        }

        return new StringVector(buffer, position, nullBuffer, size);
    }

    // CSOFF
    private static UTF8String getString(ByteBuffer buffer, int position)
    // CSON
    {
        int length = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(length);
        return UTF8String.utf8(buffer.array(), position, length);
    }
}
