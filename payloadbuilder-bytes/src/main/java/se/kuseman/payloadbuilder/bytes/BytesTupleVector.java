package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Implementation of a {@link TupleVector} that uses a byte array as underlying data. */
class BytesTupleVector implements TupleVector
{
    private final Schema schema;
    private final Schema payloadSchema;
    private final ByteBuffer buffer;
    private final ReadContext context;
    private final int startPosition;
    private final int columnCount;
    private final int rowCount;

    BytesTupleVector(Schema schema, Schema payloadSchema, ByteBuffer buffer, ReadContext context, int columnCount, int rowCount, int startPosition)
    {
        this.schema = schema;
        this.payloadSchema = payloadSchema;
        this.buffer = buffer;
        this.context = context;
        this.columnCount = columnCount;
        this.rowCount = rowCount;
        this.startPosition = startPosition;
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public ValueVector getColumn(int column)
    {
        // Always use the payload schema if exists to be consistent with the payloads type
        // These one can differ if the input schema doesn't match with the payloads
        Schema schema = payloadSchema != null ? payloadSchema
                : this.schema;

        Column col = schema.getColumns()
                .get(column);

        // Physical slot this column's data actually lives at in the payload. Absent (null) metadata means
        // no column-id based remap happened for this schema, so the logical column index is the physical slot
        // as-is - this is the pre-existing, unchanged behavior. A physical slot of -1 means this column was
        // matched by id against the expected schema but isn't present in this particular payload at all (added
        // to the expected schema after this payload was written, or removed from the writer's schema).
        Object physicalSlotMeta = col.getMetaData()
                .getMetaData(Utils.PHYSICAL_SLOT_KEY);
        int physicalSlot = physicalSlotMeta == null ? column
                : (int) physicalSlotMeta;

        // The schema has more columns that the payload physically has => null since this payload is "old"
        // comparing to the provided schema (or the column was matched by id but missing from this payload)
        if (physicalSlot < 0
                || physicalSlot >= columnCount)
        {
            return ValueVector.literalNull(col.getType(), rowCount);
        }

        int columnOffset = startPosition + (physicalSlot * AVector.REFERENCE_HEADER_SIZE);
        int columnDataPosition = buffer.getInt(columnOffset);

        return VectorFactory.getVector(buffer, columnDataPosition, context, col.getType());
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }
}
