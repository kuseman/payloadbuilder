package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter.WriterSettings;

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
    protected Encoding getEncoding(ValueVector vector, int from, int to, WriterSettings settings)
    {
        boolean firstSet = false;
        long value = -1;
        for (int i = from + 0; i < to; i++)
        {
            if (vector.isNull(i))
            {
                return Encoding.REGULAR;
            }
            else if (!firstSet)
            {
                value = vector.getDateTime(i)
                        .getEpoch();
                firstSet = true;
            }
            else if (value != vector.getDateTime(i)
                    .getEpoch())
            {
                return Encoding.REGULAR;
            }
        }
        return Encoding.REGULAR_LITERAL;
    }

    @Override
    protected int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
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
