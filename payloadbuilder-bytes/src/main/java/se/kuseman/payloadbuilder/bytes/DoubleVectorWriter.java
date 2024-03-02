package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

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
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        double value = vector.getDouble(from);
        for (int i = from + 1; i < to; i++)
        {
            if (value != vector.getDouble(i))
            {
                return false;
            }
        }
        return true;
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
