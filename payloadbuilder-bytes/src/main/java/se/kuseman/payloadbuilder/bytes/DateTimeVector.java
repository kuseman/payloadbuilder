package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Date time vector */
class DateTimeVector extends AVector
{
    static final byte VERSION = 1;

    DateTimeVector(ByteBuffer buffer, int startPosition, NullBuffer nullBuffer, int size)
    {
        super(buffer, ResolvedType.of(Type.DateTime), size, nullBuffer, startPosition);
    }

    @Override
    public EpochDateTime getDateTime(int row)
    {
        int offset = dataStartPosition + (row * REFERENCE_HEADER_SIZE);
        int valueOffset = buffer.getInt(offset);
        return EpochDateTime.from(buffer.getLong(valueOffset));
    }

    /** Create date time vector vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, NullBuffer nullBuffer, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(DateTimeVector.class, version);
        }

        int encoding = buffer.get(position++);
        if (encoding == PayloadReader.LITERAL_ENCODING)
        {
            int valueOffset = buffer.getInt(position);
            return ValueVector.literalDateTime(EpochDateTime.from(buffer.getLong(valueOffset)), size);
        }

        return new DateTimeVector(buffer, position, nullBuffer, size);
    }
}
