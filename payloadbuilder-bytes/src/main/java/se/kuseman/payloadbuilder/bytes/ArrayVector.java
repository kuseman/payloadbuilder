package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Array vector */
class ArrayVector extends AVector
{
    static final byte VERSION = 1;
    private final ReadContext context;

    ArrayVector(ByteBuffer buffer, ReadContext context, int startPosition, NullBuffer nullBuffer, ResolvedType type, int size)
    {
        super(buffer, type, size, nullBuffer, startPosition);
        this.context = context;
    }

    @Override
    public ValueVector getArray(int row)
    {
        int offset = dataStartPosition + (row * Integer.BYTES);
        int dataOffset = buffer.getInt(offset);
        return VectorFactory.getVector(buffer, dataOffset, context, type.getSubType());
    }

    /** Create array vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, ReadContext context, NullBuffer nullBuffer, ResolvedType type, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(ArrayVector.class, version);
        }

        position++; // Read past encoding, not used atm.
        return new ArrayVector(buffer, context, position, nullBuffer, type, size);
    }
}
