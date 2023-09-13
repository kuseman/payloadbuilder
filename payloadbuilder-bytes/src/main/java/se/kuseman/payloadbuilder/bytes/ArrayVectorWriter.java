package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Writer that writes Vectors of type {@link Column.Type#Array} */
class ArrayVectorWriter extends AReferenceVectorWriter
{
    static ArrayVectorWriter INSTANCE = new ArrayVectorWriter();

    @Override
    public byte getVersion()
    {
        return ArrayVector.VERSION;
    }

    @Override
    protected boolean isLiteral(ValueVector vector, int from, int to)
    {
        return false;
    }

    @Override
    protected int getAndCachedPosition(BytesWriter writer, WriteCache cache, ValueVector vector, int row)
    {
        int position = writer.position();

        // Since we don't value vectors yet we simply write the vector and return the data position
        ValueVector valueVector = vector.getArray(row);
        PayloadWriter.writeVector(writer, cache, valueVector, 0, valueVector.size());

        return position;
    }
}
