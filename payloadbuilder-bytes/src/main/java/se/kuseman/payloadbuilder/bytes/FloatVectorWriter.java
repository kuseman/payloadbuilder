package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer of {@link Column.Type#Float} */
class FloatVectorWriter implements VectorWriter
{
    static FloatVectorWriter INSTANCE = new FloatVectorWriter();

    private FloatVectorWriter()
    {
    }

    @Override
    public byte getVersion()
    {
        return FloatVector.VERSION;
    }

    @Override
    public void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount)
    {
        // Find out if we have a literal vector
        if (nullCount == 0)
        {
            boolean literal = true;
            float value = vector.getFloat(from);
            for (int i = from + 1; i < to; i++)
            {
                if (value != vector.getFloat(i))
                {
                    literal = false;
                    break;
                }
            }

            if (literal)
            {
                writer.putByte(PayloadReader.LITERAL_ENCODING);
                // Literal float then we have the literal value in the data position
                writer.putFloat(value);
                return;
            }
        }

        writer.putByte(PayloadReader.REGULAR_ENCODING);
        for (int i = from; i < to; i++)
        {
            boolean isNull = vector.isNull(i);
            float value = isNull ? 0
                    : vector.getFloat(i);
            writer.putFloat(value);
        }
    }
}
