package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter.WriterSettings;

/** Writer of {@link Column.Type#Long} */
class LongVectorWriter extends AReferenceVectorWriter
{
    static LongVectorWriter INSTANCE = new LongVectorWriter();

    @Override
    public byte getVersion()
    {
        return LongVector.VERSION;
    }

    @Override
    protected Encoding getEncoding(ValueVector vector, int from, int to, WriterSettings settings)
    {
        boolean firstValue = false;
        long value = -1;
        for (int i = from + 0; i < to; i++)
        {
            if (vector.isNull(i))
            {
                return Encoding.REGULAR;
            }
            else if (!firstValue)
            {
                value = vector.getLong(i);
                firstValue = true;
            }
            else if (value != vector.getLong(i))
            {
                return Encoding.REGULAR;
            }
        }
        return Encoding.REGULAR_LITERAL;
    }

    @Override
    protected int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        long value = vector.getLong(row);
        Integer position = cache.getLongPosition(value);
        if (position == null)
        {
            position = writer.position();
            writer.putLong(value);
            cache.putLongPosition(value, position);
        }
        return position;
    }
}
