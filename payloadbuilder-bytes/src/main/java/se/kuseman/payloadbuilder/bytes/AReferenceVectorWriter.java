package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A writer that writes vectors who's items are references to other places in the buffer */
abstract class AReferenceVectorWriter implements VectorWriter
{
    @Override
    public void write(BytesWriter writer, WriteCache cache, ValueVector vector, int from, int to, int nullCount)
    {
        // Find out if we have a literal vector
        if (nullCount == 0)
        {
            boolean literal = isLiteral(vector, from, to);

            if (literal)
            {
                writer.putByte(PayloadReader.LITERAL_ENCODING);

                int valueOffset = writer.position();

                // Set writer to position after literl data
                writer.position(valueOffset + AVector.REFERENCE_HEADER_SIZE);

                // Get cached position
                int position = getAndCachePosition(writer, cache, vector, from);
                writer.putInt(valueOffset, position);
                return;
            }
        }

        writer.putByte(PayloadReader.REGULAR_ENCODING);

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

    /** Returns true if vector is literal */
    protected abstract boolean isLiteral(ValueVector vector, int from, int to);

    /** Get and cache position of provided row */
    protected abstract int getAndCachePosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row);
}
