package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Float vector */
class FloatVector extends AVector
{
    static final byte VERSION = 1;

    FloatVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.Float), size, nullBuffer, startPosition);
    }

    @Override
    public float getFloat(int row)
    {
        int offset = dataStartPosition + (row * Float.BYTES);
        return buffer.getFloat(offset);
    }

    /** Create float vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(FloatVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            float value = buffer.getFloat(position);
            return ValueVector.literalFloat(value, size);
        }

        return new FloatVector(buffer, position, nullBuffer, size);
    }
}
