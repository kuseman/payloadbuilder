package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** ObjectVector vector */
class ObjectVector implements ValueVector
{
    static final byte VERSION = 1;
    private final ResolvedType type;
    private final NullBuffer nullBuffer;
    private final TupleVector tupleVector;
    private final int size;

    ObjectVector(NullBuffer nullBuffer, ResolvedType type, int size, TupleVector tupleVector)
    {
        this.nullBuffer = nullBuffer;
        this.type = type;
        this.size = size;
        this.tupleVector = tupleVector;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public ResolvedType type()
    {
        return type;
    }

    @Override
    public boolean isNull(int row)
    {
        return nullBuffer.isNull(row);
    }

    @Override
    public se.kuseman.payloadbuilder.api.execution.ObjectVector getObject(int row)
    {
        return se.kuseman.payloadbuilder.api.execution.ObjectVector.wrap(tupleVector, row);
    }

    /** Create tuple vector vector. */
    static ValueVector getVector(ByteBuffer buffer, int position, ReadContext context, NullBuffer nullBuffer, ResolvedType type, ResolvedType payloadType, byte version, int size)
    {
        if (version != VERSION)
        {
            AVector.throwUnknownVersion(ObjectVector.class, version);
        }

        position++; // Read past encoding, not used atm.
        int columnCount = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(columnCount);

        // Read the tuple vector
        int rowCount = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(rowCount);

        TupleVector tupleVector = new BytesTupleVector(type.getSchema(), payloadType != null ? payloadType.getSchema()
                : null, buffer, context, columnCount, rowCount, position);

        return new ObjectVector(nullBuffer, type, rowCount, tupleVector);
    }
}
