package se.kuseman.payloadbuilder.core.execution.vector;

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

/** A mutable long value vector */
class MutableObjectVector extends AMutableVector
{
    private Object[] buffer;
    private boolean hasNulls;

    MutableObjectVector(VectorFactory factory, int estimatedCapacity, ResolvedType type)
    {
        super(factory, estimatedCapacity, type);
    }

    @Override
    public void setNull(int row)
    {
        size = Math.max(size, row + 1);
        ensureSize(row + 1);
        buffer[row] = null;
        hasNulls = true;
    }

    @Override
    public boolean hasNulls()
    {
        return hasNulls;
    }

    @Override
    public boolean isNull(int row)
    {
        if (buffer == null)
        {
            return true;
        }
        return buffer[row] == null;
    }

    @Override
    public void setAny(int row, Object value)
    {
        size = Math.max(size, row + 1);
        ensureSize(row + 1);
        if (value instanceof String)
        {
            value = UTF8String.from(value);
        }
        buffer[row] = value;
        hasNulls = hasNulls
                || value == null;
    }

    @Override
    public void setArray(int row, ValueVector value)
    {
        setAny(row, value);
    }

    @Override
    public void setObject(int row, ObjectVector value)
    {
        setAny(row, value);
    }

    @Override
    public void setTable(int row, TupleVector value)
    {
        setAny(row, value);
    }

    @Override
    public void setDecimal(int row, Decimal value)
    {
        setAny(row, value);
    }

    @Override
    public void setDateTime(int row, EpochDateTime value)
    {
        setAny(row, value);
    }

    @Override
    public void setDateTimeOffset(int row, EpochDateTimeOffset value)
    {
        setAny(row, value);
    }

    @Override
    public void setString(int row, UTF8String value)
    {
        setAny(row, value);
    }

    @Override
    public void copy(int startRow, ValueVector source, int sourceRow, int length)
    {
        int newSize = startRow + length;
        ensureSize(newSize);
        size = Math.max(size, newSize);
        switch (type.getType())
        {
            case String:
                copyString(startRow, source, sourceRow, length);
                break;
            case Decimal:
                copyDecimal(startRow, source, sourceRow, length);
                break;
            case DateTime:
                copyDateTime(startRow, source, sourceRow, length);
                break;
            case DateTimeOffset:
                copyDateTimeOffset(startRow, source, sourceRow, length);
                break;
            case Any:
                copyAny(startRow, source, sourceRow, length);
                break;
            case Array:
                copyArray(startRow, source, sourceRow, length);
                break;
            case Object:
                copyObject(startRow, source, sourceRow, length);
                break;
            case Table:
                copyTable(startRow, source, sourceRow, length);
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type.getType());
        }
    }

    @Override
    public float getFloat(int row)
    {
        if (type.getType() == Column.Type.String)
        {
            String str = ((UTF8String) buffer[row]).toString();
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

        return buffer[row];
    }

    @Override
    public ValueVector getArray(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Array)
        {
            return super.getArray(row);
        }

        return (ValueVector) buffer[row];
    }

    @Override
    public TupleVector getTable(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Table)
        {
            return super.getTable(row);
        }

        return (TupleVector) buffer[row];
    }

    @Override
    public ObjectVector getObject(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Object)
        {
            return super.getObject(row);
        }

        return (ObjectVector) buffer[row];
    }

    @Override
    public EpochDateTime getDateTime(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.DateTime)
        {
            return super.getDateTime(row);
        }

        return (EpochDateTime) buffer[row];
    }

    @Override
    public EpochDateTimeOffset getDateTimeOffset(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.DateTimeOffset)
        {
            return super.getDateTimeOffset(row);
        }

        return (EpochDateTimeOffset) buffer[row];
    }

    @Override
    public Decimal getDecimal(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.Decimal)
        {
            return super.getDecimal(row);
        }

        return (Decimal) buffer[row];
    }

    @Override
    public UTF8String getString(int row)
    {
        // Implicit cast
        if (type.getType() != Column.Type.String)
        {
            return super.getString(row);
        }

        return (UTF8String) buffer[row];
    }

    private void copyString(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getString(sr);
            }
        }
    }

    private void copyDecimal(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getDecimal(sr);
            }
        }
    }

    private void copyDateTime(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getDateTime(sr);
            }
        }
    }

    private void copyDateTimeOffset(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getDateTimeOffset(sr);
            }
        }
    }

    private void copyArray(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getArray(sr);
            }
        }
    }

    private void copyObject(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getObject(sr);
            }
        }
    }

    private void copyTable(int startRow, ValueVector source, int sourceRow, int length)
    {
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getTable(sr);
            }
        }
    }

    private void copyAny(int startRow, ValueVector source, int sourceRow, int length)
    {
        boolean hasNulls = source.hasNulls();
        for (int i = 0; i < length; i++)
        {
            int sr = sourceRow + i;
            int dr = startRow + i;
            if (hasNulls
                    && source.isNull(sr))
            {
                setNull(dr);
            }
            else
            {
                buffer[dr] = source.getAny(sr);
            }
        }
    }

    private void ensureSize(int limit)
    {
        if (buffer == null)
        {
            buffer = factory.getAllocator()
                    .getObjectBuffer(Math.max(estimatedCapacity, limit));
        }
        else if (buffer.length < limit)
        {
            Object[] newBuffer = factory.getAllocator()
                    .getObjectBuffer(Math.max(estimatedCapacity, limit * 2));
            System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
            buffer = newBuffer;
        }
    }
}
