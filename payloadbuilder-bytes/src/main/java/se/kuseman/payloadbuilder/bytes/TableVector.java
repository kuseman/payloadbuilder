package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Table vector */
class TableVector extends AVector
{
    static final byte VERSION = 1;
    private final int columnCount;
    private final Schema schema;
    private final ReadContext context;

    TableVector(ByteBuffer buffer, ReadContext context, int startPosition, NullBuffer nullBuffer, ResolvedType type, int size, int columnCount)
    {
        super(buffer, type, size, nullBuffer, startPosition);
        this.columnCount = columnCount;
        this.schema = type.getSchema();
        this.context = context;
    }

    @Override
    public TupleVector getTable(int row)
    {
        int offset = dataStartPosition + (row * Integer.BYTES);
        int dataOffset = buffer.getInt(offset);

        int rowCount = Utils.readVarInt(buffer, dataOffset);

        offset = dataOffset + Utils.sizeOfVarInt(rowCount);
        return new BytesTupleVector(schema, buffer, context, columnCount, rowCount, offset);
    }

    /** Create tuple vector vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, ReadContext context, NullBuffer nullBuffer, ResolvedType type, byte version, int size)
    {
        if (version != VERSION)
        {
            throwUnknownVersion(TableVector.class, version);
        }

        position++; // Read past encoding, not used atm.
        int columnCount = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(columnCount);

        return new TableVector(buffer, context, position, nullBuffer, type, size, columnCount);
    }
}
