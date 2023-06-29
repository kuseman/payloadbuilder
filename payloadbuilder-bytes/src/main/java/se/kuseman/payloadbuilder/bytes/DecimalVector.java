package se.kuseman.payloadbuilder.bytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Decimal vector */
class DecimalVector extends AVector
{
    static final byte VERSION = 1;
    private final ReadContext context;

    DecimalVector(ByteBuffer buffer, ReadContext context, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.Decimal), size, nullBuffer, startPosition);
        this.context = context;
    }

    @Override
    public Decimal getDecimal(int row)
    {
        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        return getDecimal(buffer, context, valueOffset);
    }

    /** Create string vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, ReadContext context, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(DecimalVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalDecimal(getDecimal(buffer, context, valueOffset), size);
        }

        return new DecimalVector(buffer, context, position, nullBuffer, size);
    }

    // CSOFF
    private static Decimal getDecimal(ByteBuffer buffer, ReadContext context, int position)
    // CSON
    {
        return Decimal.from(context.computeBigDecimal(position, () ->
        {
            int pos = position;
            int length = Utils.readVarInt(buffer, pos);
            pos += Utils.sizeOfVarInt(length);
            byte[] bytes = new byte[length];

            for (int i = 0; i < length; i++)
            {
                bytes[i] = buffer.get(pos++);
            }

            int scale = Utils.readVarInt(buffer, pos);
            // TODO: In java 8 BigIntegers cannot be created with a byte-array with offset + length
            return new BigDecimal(new BigInteger(bytes), scale);
        }));
    }
}
