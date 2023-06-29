package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Boolean vector */
class BooleanVector extends AVector
{
    static final byte VERSION = 1;

    BooleanVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.Boolean), size, nullBuffer, startPosition);
    }

    @Override
    public boolean getBoolean(int row)
    {
        byte b = buffer.get(dataStartPosition + row / 8);
        return (b & (1 << (row % 8))) != 0;
    }

    /** Create boolean vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(BooleanVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            byte value = buffer.get(position);
            return ValueVector.literalBoolean(value == 1, size);
        }

        // Read past the bytes needed count
        int bytesNeeded = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(bytesNeeded);

        return new BooleanVector(buffer, position, nullBuffer, size);
    }
}
