package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer of {@link Column.Type#String} */
class StringVectorWriter extends AReferenceVectorWriter
{
    static StringVectorWriter INSTANCE = new StringVectorWriter();

    @Override
    public byte getVersion()
    {
        return StringVector.VERSION;
    }

    @Override
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        UTF8String value = vector.getString(from);
        for (int i = from + 1; i < to; i++)
        {
            if (!value.equals(vector.getString(i)))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    protected int getAndCachedPosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        UTF8String value = vector.getString(row);
        Integer position = cache.getStringPosition(value);
        if (position == null)
        {
            position = writer.position();
            byte[] bytes = value.getBytes();
            writer.putVarInt(bytes.length);
            writer.putBytes(bytes);
            cache.putStringPosition(value, position);
        }
        return position;
    }
}
