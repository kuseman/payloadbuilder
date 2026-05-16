package se.kuseman.payloadbuilder.bytes;

import java.math.BigDecimal;
import java.math.BigInteger;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter.WriterSettings;

/** Writer of {@link Column.Type#Decimal} */
class DecimalVectorWriter extends AReferenceVectorWriter
{
    static DecimalVectorWriter INSTANCE = new DecimalVectorWriter();

    @Override
    public byte getVersion()
    {
        return DecimalVector.VERSION;
    }

    @Override
    protected Encoding getEncoding(ValueVector vector, int from, int to, WriterSettings settings)
    {
        Decimal value = null;
        for (int i = from + 0; i < to; i++)
        {
            if (vector.isNull(i))
            {
                return Encoding.REGULAR;
            }
            else if (value == null)
            {
                value = vector.getDecimal(i);
                continue;
            }
            else if (!value.equals(vector.getDecimal(i)))
            {
                return Encoding.REGULAR;
            }
        }
        return Encoding.REGULAR_LITERAL;
    }

    @Override
    protected int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        Decimal value = vector.getDecimal(row);
        Integer position = cache.getDecimalPosition(value);
        if (position == null)
        {
            position = writer.position();
            BigDecimal bigDecimal = value.asBigDecimal();
            BigInteger unscaledValue = bigDecimal.unscaledValue();
            int scale = bigDecimal.scale();
            byte[] bytes = unscaledValue.toByteArray();
            writer.putVarInt(bytes.length);
            writer.putBytes(bytes);
            writer.putVarInt(scale);

            cache.putDecimalPosition(value, position);
        }
        return position;
    }
}
