package se.kuseman.payloadbuilder.bytes;

import java.math.BigDecimal;
import java.math.BigInteger;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

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
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        Decimal value = vector.getDecimal(from);
        for (int i = from + 1; i < to; i++)
        {
            if (!value.equals(vector.getDecimal(i)))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    protected int getAndCachedPosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
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
