package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer of {@link Column.Type#Int} */
class IntVectorWriter implements VectorWriter
{
    static IntVectorWriter INSTANCE = new IntVectorWriter();

    private IntVectorWriter()
    {
    }

    @Override
    public byte getVersion()
    {
        return IntVector.VERSION;
    }

    @Override
    public void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount)
    {
        // Find out if we have a literal vector
        if (nullCount == 0)
        {
            boolean literal = true;
            int value = vector.getInt(from);
            for (int i = from + 1; i < to; i++)
            {
                if (value != vector.getInt(i))
                {
                    literal = false;
                    break;
                }
            }

            if (literal)
            {
                writer.putByte(PayloadReader.LITERAL_ENCODING);
                // Literal int then we have the literal value in the data position
                writer.putInt(value);
                return;
            }
        }

        writer.putByte(PayloadReader.REGULAR_ENCODING);
        for (int i = from; i < to; i++)
        {
            boolean isNull = vector.isNull(i);
            int value = isNull ? 0
                    : vector.getInt(i);
            writer.putInt(value);
        }
    }
}
