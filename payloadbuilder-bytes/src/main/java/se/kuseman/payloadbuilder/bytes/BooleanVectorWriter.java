package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer of {@link Column.Type#Boolean} */
class BooleanVectorWriter implements VectorWriter
{
    static BooleanVectorWriter INSTANCE = new BooleanVectorWriter();

    private BooleanVectorWriter()
    {
    }

    @Override
    public byte getVersion()
    {
        return BooleanVector.VERSION;
    }

    @Override
    public void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount)
    {
        // Find out if we have a literal vector
        if (nullCount == 0)
        {
            boolean literal = true;
            boolean value = vector.getBoolean(from);
            for (int i = from + 1; i < to; i++)
            {
                if (value != vector.getBoolean(i))
                {
                    literal = false;
                    break;
                }
            }

            if (literal)
            {
                writer.putByte(PayloadReader.LITERAL_ENCODING);
                // Literal boolean then we have the literal value in the data position
                writer.putByte((byte) (value ? 1
                        : 0));
                return;
            }
        }

        writer.putByte(PayloadReader.REGULAR_ENCODING);

        int bytesNeeded = (int) Math.ceil((to - from) / 8.0);
        writer.putVarInt(bytesNeeded);

        byte current = 0;
        int index = 0;
        for (int i = from; i < to; i++)
        {
            boolean isNull = vector.isNull(i);
            boolean value = isNull ? false
                    : vector.getBoolean(i);
            if (value)
            {
                current |= 1 << index;
            }

            index++;
            if (index >= 8
                    || i == to - 1)
            {
                writer.putByte(current);
                current = 0;
                index = 0;
            }
        }
    }
}
