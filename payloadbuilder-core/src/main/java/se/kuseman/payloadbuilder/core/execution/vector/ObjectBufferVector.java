package se.kuseman.payloadbuilder.core.execution.vector;

import static java.util.Objects.requireNonNull;

import java.util.List;

import com.fasterxml.jackson.core.io.doubleparser.JavaFloatParser;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Buffer vector for types that is non primitives */
class ObjectBufferVector extends ABufferVector
{
    private final List<?> buffer;

    ObjectBufferVector(List<?> buffer, ResolvedType type, int startPosition, int size)
    {
        super(type, size, null, startPosition);
        this.buffer = requireNonNull(buffer, "buffer");
    }

    @Override
    public boolean isNull(int row)
    {
        return buffer.get(startPosition + row) == null;
    }

    @Override
    public float getFloat(int row)
    {
        if (type.getType() == Column.Type.String)
        {
            String str = ((UTF8String) buffer.get(startPosition + row)).toString();
            return JavaFloatParser.parseFloat(str);
        }

        // Implicit cast
        return super.getFloat(row);
    }

    @Override
    public Object getAny(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Any)
        {
            return super.getAny(row);
        }

        return buffer.get(startPosition + row);
    }

    @Override
    public ValueVector getArray(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Array)
        {
            return super.getArray(row);
        }

        return (ValueVector) buffer.get(startPosition + row);
    }

    @Override
    public TupleVector getTable(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Table)
        {
            return super.getTable(row);
        }

        return (TupleVector) buffer.get(startPosition + row);
    }

    @Override
    public ObjectVector getObject(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Object)
        {
            return super.getObject(row);
        }

        return (ObjectVector) buffer.get(startPosition + row);
    }

    @Override
    public EpochDateTime getDateTime(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.DateTime)
        {
            return super.getDateTime(row);
        }

        return (EpochDateTime) buffer.get(startPosition + row);
    }

    @Override
    public EpochDateTimeOffset getDateTimeOffset(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.DateTimeOffset)
        {
            return super.getDateTimeOffset(row);
        }

        return (EpochDateTimeOffset) buffer.get(startPosition + row);
    }

    @Override
    public Decimal getDecimal(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Decimal)
        {
            return super.getDecimal(row);
        }

        return (Decimal) buffer.get(startPosition + row);
    }

    @Override
    public UTF8String getString(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.String)
        {
            return super.getString(row);
        }

        return (UTF8String) buffer.get(startPosition + row);
    }
}
