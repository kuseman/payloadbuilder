package se.kuseman.payloadbuilder.core.execution.vector;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link ValueVector} for booleans backed with {@link BitBuffer} */
class BooleanBufferVector extends ABufferVector
{
    private final BitBuffer buffer;

    public BooleanBufferVector(BitBuffer buffer, int startPosition, int size, BitBuffer nullBuffer)
    {
        super(ResolvedType.of(Type.Boolean), size, nullBuffer, startPosition);
        this.buffer = buffer;
    }

    @Override
    public boolean getBoolean(int row)
    {
        return buffer.get(startPosition + row);
    }
}
