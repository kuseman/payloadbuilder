package se.kuseman.payloadbuilder.core.execution.vector;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Base class for buffer vectors */
abstract class ABufferVector implements ValueVector
{
    protected final ResolvedType type;
    private final int size;
    protected final BitBuffer nullBuffer;
    protected final int startPosition;

    ABufferVector(ResolvedType type, int size, BitBuffer nullBuffer, int startPosition)
    {
        this.type = type;
        this.size = size;
        this.nullBuffer = nullBuffer;
        this.startPosition = startPosition;
    }

    @Override
    public ResolvedType type()
    {
        return type;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isNull(int row)
    {
        if (nullBuffer == null)
        {
            return false;
        }
        return nullBuffer.get(startPosition + row);
    }
}
