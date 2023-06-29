package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

/** Null buffer that reads null bits from bytes */
class NullBuffer
{
    private static final NullBuffer NO_NULL = new NoNullBuffer();
    private static final NullBuffer ALL_NULL = new AllNullBuffer();
    private final ByteBuffer buffer;
    private final int startPosition;

    NullBuffer(ByteBuffer buffer, int startPosition)
    {
        this.buffer = buffer;
        this.startPosition = startPosition;
    }

    boolean isNull(int row)
    {
        byte b = buffer.get(startPosition + row / 8);
        return (b & (1 << (row % 8))) != 0;
    }

    /** Return true if all is null else false */
    boolean isAllNull()
    {
        return false;
    }

    /**
     * Reads and sets a null buffer in provided ref.
     *
     * @return Returns the position after null buffer data
     */
    static int getNullBuffer(ByteBuffer buffer, int position, int vectorSize, Reference<NullBuffer> ref)
    {
        int nullByteLength = Utils.readVarInt(buffer, position);
        int sizeOfLength = Utils.sizeOfVarInt(nullByteLength);

        int startPosition = position + sizeOfLength;
        // Special case where byte length equals vector size => all items are null
        if (nullByteLength == vectorSize)
        {
            ref.set(ALL_NULL);
            return startPosition;
        }
        else if (nullByteLength == 0)
        {
            ref.set(NO_NULL);
            return startPosition;
        }
        ref.set(new NullBuffer(buffer, startPosition));
        return startPosition + nullByteLength;
    }

    static class NoNullBuffer extends NullBuffer
    {
        NoNullBuffer()
        {
            super(null, -1);
        }

        @Override
        boolean isNull(int row)
        {
            return false;
        }
    }

    static class AllNullBuffer extends NullBuffer
    {
        AllNullBuffer()
        {
            super(null, -1);
        }

        @Override
        boolean isAllNull()
        {
            return true;
        }

        @Override
        boolean isNull(int row)
        {
            return true;
        }
    }
}
