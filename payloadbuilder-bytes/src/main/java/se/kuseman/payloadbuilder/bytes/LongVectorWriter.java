package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

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
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        long value = vector.getLong(from);
        for (int i = from + 1; i < to; i++)
        {
            if (value != vector.getLong(i))
            {
                return false;
            }
        }
        return true;
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
