package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.execution.ValueVector;

class NullBufferWriter
{
    private NullBufferWriter()
    {
    }

    /**
     * Writes a null buffer for provided vector at writers current position.
     *
     * @return Return number of nulls written
     */
    static int writeNullBuffer(BytesWriter writer, ValueVector vector, int from, int to)
    {
        int rowCount = to - from;

        boolean anyNulls = false;
        boolean allNulls = true;
        for (int i = from; i < to; i++)
        {
            boolean isNull = vector.isNull(i);
            anyNulls = anyNulls
                    || isNull;
            allNulls = allNulls
                    && isNull;
        }

        if (allNulls)
        {
            // Special case for Literal null, null byte length = rowcount
            writer.putVarInt(rowCount);
            return rowCount;
        }
        else if (!anyNulls)
        {
            writer.putVarInt(0);
            return 0;
        }

        int bytesNeeded = (int) Math.ceil(rowCount / 8.0);
        writer.putVarInt(bytesNeeded);

        int nullCount = 0;
        byte current = 0;
        int index = 0;
        for (int i = from; i < to; i++)
        {
            boolean isNull = vector.isNull(i);
            if (isNull)
            {
                current |= 1 << index;
                nullCount++;
            }

            index++;
            if (index >= 8
                    || i == rowCount - 1)
            {
                writer.putByte(current);
                current = 0;
                index = 0;
            }
        }

        return nullCount;
    }

}
