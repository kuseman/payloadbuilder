package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** String vector */
class StringVector extends AVector
{
    static final byte VERSION = 1;
    static final byte LATIN1_LITERAL_ENCODING = 2;
    static final byte LATIN1_ENCODING = 3;
    private final boolean latin1;

    StringVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size, boolean latin1)
    {
        super(buffer, ResolvedType.of(Type.String), size, nullBuffer, startPosition);
        this.latin1 = latin1;
    }

    /** Create a java.lang.String directly from bytes without going through UTF8String. */
    @Override
    public String valueAsString(int row)
    {
        if (isNull(row))
        {
            return null;
        }

        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        int length = Utils.readVarInt(buffer, valueOffset);
        valueOffset += Utils.sizeOfVarInt(length);
        return new String(buffer.array(), valueOffset, length, latin1 ? StandardCharsets.ISO_8859_1
                : StandardCharsets.UTF_8);
    }

    @Override
    public UTF8String getString(int row)
    {
        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        return getString(buffer, valueOffset, latin1);
    }

    /** Create string vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(StringVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.REGULAR_LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalString(getString(buffer, valueOffset, false), size);
        }
        else if (encoding == LATIN1_LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalString(getString(buffer, valueOffset, true), size);
        }

        return new StringVector(buffer, position, nullBuffer, size, encoding == LATIN1_ENCODING);
    }

    // CSOFF
    private static UTF8String getString(ByteBuffer buffer, int position, boolean latin1)
    // CSON
    {
        int length = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(length);
        return latin1 ? UTF8String.latin(buffer.array(), position, length)
                : UTF8String.utf8(buffer.array(), position, length);
    }
}
