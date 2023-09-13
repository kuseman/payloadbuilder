package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Implementation of a {@link TupleVector} that uses a byte array as underlying data. */
class BytesTupleVector implements TupleVector
{
    private final Schema schema;
    private final ByteBuffer buffer;
    private final ReadContext context;
    private final int startPosition;
    private final int columnCount;
    private final int rowCount;

    BytesTupleVector(Schema schema, ByteBuffer buffer, ReadContext context, int columnCount, int rowCount, int startPosition)
    {
        this.buffer = buffer;
        this.context = context;
        this.columnCount = columnCount;
        this.rowCount = rowCount;
        this.startPosition = startPosition;
        this.schema = schema;
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public ValueVector getColumn(int column)
    {
        // The schema has more columns that the payload => null since this payload is "old"
        // comparing to the provided schema
        if (column >= columnCount)
        {
            return ValueVector.literalNull(schema.getColumns()
                    .get(column)
                    .getType(), rowCount);
        }

        int columnOffset = startPosition + (column * AVector.REFERENCE_HEADER_SIZE);
        int columnDataPosition = buffer.getInt(columnOffset);
        return VectorFactory.getVector(buffer, columnDataPosition, context, schema.getColumns()
                .get(column)
                .getType());
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }
}
