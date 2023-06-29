package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer of {@link Column.Type#DateTime} */
class DateTimeVectorWriter extends AReferenceVectorWriter
{
    static DateTimeVectorWriter INSTANCE = new DateTimeVectorWriter();

    @Override
    public byte getVersion()
    {
        return DateTimeVector.VERSION;
    }

    @Override
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        long value = vector.getDateTime(from)
                .getEpoch();
        for (int i = from + 1; i < to; i++)
        {
            if (value != vector.getDateTime(i)
                    .getEpoch())
            {
                return false;
            }
        }
        return true;
    }

    @Override
    protected int getAndCachedPosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        long value = vector.getDateTime(row)
                .getEpoch();
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
