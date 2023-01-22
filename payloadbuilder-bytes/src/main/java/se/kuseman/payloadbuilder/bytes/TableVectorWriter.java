package se.kuseman.payloadbuilder.bytes;

import static se.kuseman.payloadbuilder.bytes.AVector.REFERENCE_HEADER_SIZE;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer that writes Vectors of type {@link Column.Type#Table} */
class TableVectorWriter extends AReferenceVectorWriter
{
    static TableVectorWriter INSTANCE = new TableVectorWriter();

    @Override
    public byte getVersion()
    {
        return TableVector.VERSION;
    }

    @Override
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        return false;
    }

    @Override
    protected void writeMeta(BytesWriter writer, ValueVector vector)
    {
        int size = vector.type()
                .getSchema()
                .getSize();

        writer.putVarInt(size);
    }

    @Override
    protected int getAndCachedPosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        int position = writer.position();

        int size = vector.type()
                .getSchema()
                .getSize();

        // Since we don't cache tuple vectors yet we simply write the vector and return the data position
        TupleVector tupleVector = vector.getTable(row);
        writeTupleVector(writer, cache, tupleVector, size);

        return position;
    }

    /** Write tuple vector at writer position. NOTE! This method is used from ObjectVectorWriter as well */
    static void writeTupleVector(BytesWriter writer, WriteCache cache, TupleVector tupleVector, int columnCount)
    {
        int rowCount = tupleVector.getRowCount();
        writer.putVarInt(rowCount);

        // Allocate 4 bytes for each column
        int headerSize = columnCount * REFERENCE_HEADER_SIZE;

        int headerPosition = writer.position();

        // Set writer as data position before writing
        writer.position(headerPosition + headerSize);

        for (int col = 0; col < columnCount; col++)
        {
            int headerOffset = headerPosition + (col * REFERENCE_HEADER_SIZE);

            // Write down the current columns data position at current header offset
            writer.putInt(headerOffset, writer.position());
            PayloadWriter.writeVector(writer, cache, tupleVector.getColumn(col), 0, rowCount);
        }
    }
}
