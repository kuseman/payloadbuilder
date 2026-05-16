package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter.WriterSettings;

/** Writer of {@link Column.Type#String} */
class StringVectorWriter extends AReferenceVectorWriter
{
    static StringVectorWriter INSTANCE = new StringVectorWriter();
    static final Encoding LITERAL_LATIN1_ENCODING = new Encoding(StringVector.LATIN1_LITERAL_ENCODING, true);
    static final Encoding LATIN1_ENCODING = new Encoding(StringVector.LATIN1_ENCODING, false);

    @Override
    public byte getVersion()
    {
        return StringVector.VERSION;
    }

    @Override
    protected Encoding getEncoding(ValueVector vector, int from, int to, WriterSettings settings)
    {
        boolean detectLatin1 = settings.isUseLatin1EncodedStrings();
        UTF8String value = null;
        boolean isLatin1 = detectLatin1;
        boolean isLiteral = true;
        for (int i = from + 0; i < to; i++)
        {
            if (vector.isNull(i))
            {
                isLiteral = false;
                continue;
            }

            UTF8String current = vector.getString(i);
            if (detectLatin1
                    && isLatin1)
            {
                byte[] bytes = current.getBytes();
                isLatin1 = UTF8String.detectLatin1(bytes, 0, bytes.length);
            }

            if (value == null)
            {
                value = current;
                continue;
            }
            else if (!value.equals(current))
            {
                isLiteral = false;
            }
        }
        if (isLiteral)
        {
            return isLatin1 ? LITERAL_LATIN1_ENCODING
                    : Encoding.REGULAR_LITERAL;
        }

        return isLatin1 ? LATIN1_ENCODING
                : Encoding.REGULAR;
    }

    @Override
    protected int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
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
