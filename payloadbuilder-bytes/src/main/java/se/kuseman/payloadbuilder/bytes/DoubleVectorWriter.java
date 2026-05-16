package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter.WriterSettings;

/** Writer of {@link Column.Type#Double} */
class DoubleVectorWriter extends AReferenceVectorWriter
{
    static DoubleVectorWriter INSTANCE = new DoubleVectorWriter();

    @Override
    public byte getVersion()
    {
        return DoubleVector.VERSION;
    }

    @Override
    protected Encoding getEncoding(ValueVector vector, int from, int to, WriterSettings settings)
    {
        boolean firstValue = false;
        double value = -1;
        for (int i = from + 0; i < to; i++)
        {
            if (vector.isNull(i))
            {
                return Encoding.REGULAR;
            }
            else if (!firstValue)
            {
                value = vector.getDouble(i);
                firstValue = true;
            }
            else if (value != vector.getDouble(i))
            {
                return Encoding.REGULAR;
            }
        }
        return Encoding.REGULAR_LITERAL;
    }

    @Override
    protected int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        double value = vector.getDouble(row);
        Integer position = cache.getDoublePosition(value);
        if (position == null)
        {
            position = writer.position();
            writer.putDouble(value);
            cache.putDoublePosition(value, position);
        }
        return position;
    }
}
