package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.bytes.PayloadWriter.WriterSettings;

/** A writer that writes vectors who's items are references to other places in the buffer */
abstract class AReferenceVectorWriter implements VectorWriter
{
    @Override
    public void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount)
    {
        Encoding encoding = getEncoding(vector, from, to, cache.getSettings());

        // Find out if we have a literal vector
        if (nullCount == 0
                && encoding.isLiteral)
        {
            writer.putByte(encoding.encoding);

            int valueOffset = writer.position();

            // Set writer to position after literl data
            writer.position(valueOffset + AVector.REFERENCE_HEADER_SIZE);

            // Get cached position
            int position = getAndCachePosition(writer, cache, vector, from);
            writer.putInt(valueOffset, position);
            return;
        }

        writer.putByte(encoding.encoding);

        writeMeta(writer, vector);

        // Allocate 4 bytes per item
        int headerSize = AVector.REFERENCE_HEADER_SIZE * (to - from);
        int headerPosition = writer.position();
        int dataPosition = headerPosition + headerSize;

        // Place the writer after all headers
        writer.position(dataPosition);

        for (int i = from; i < to; i++)
        {
            int headerOffset = headerPosition + (i * AVector.REFERENCE_HEADER_SIZE);

            boolean isNull = vector.isNull(i);
            if (isNull)
            {
                // Set 0 in header offset
                writer.putInt(headerOffset, 0);
                continue;
            }

            int position = getAndCachePosition(writer, cache, vector, i);
            writer.putInt(headerOffset, position);
        }
    }

    /** Write custom data after encoding but before data */
    protected void writeMeta(BytesWriter writer, ValueVector vector)
    {
    }

    /**
     * Return encoding byte for this vector. Reserved encodings: 0 - REGULAR_LITERAL_ENCODING 1 - REGULAR_ENCODING
     */
    protected Encoding getEncoding(ValueVector vector, int from, int to, WriterSettings settings)
    {
        return Encoding.REGULAR;
    }

    /** Get and cache position of provided row */
    protected abstract int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row);

    record Encoding(byte encoding, boolean isLiteral)
    {

        static final Encoding REGULAR = new Encoding(PayloadReader.REGULAR_ENCODING, false);
        static final Encoding REGULAR_LITERAL = new Encoding(PayloadReader.REGULAR_LITERAL_ENCODING, true);

        Encoding
        {
            if (isLiteral
                    && encoding == PayloadReader.REGULAR_ENCODING)
            {
                throw new IllegalArgumentException("Illegal encoding byte. " + PayloadReader.REGULAR_ENCODING + " is reserved for regular encoding");
            }
            else if (!isLiteral
                    && encoding == PayloadReader.REGULAR_LITERAL_ENCODING)
            {
                throw new IllegalArgumentException("Illegal literal flag. Encoding byte: " + PayloadReader.REGULAR_LITERAL_ENCODING + " is reserved for regular literal encoding");
            }
        }
    }
}
