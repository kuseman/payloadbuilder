package se.kuseman.payloadbuilder.core.execution;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Decorator for {@link ValueVector} used to remove boiler plate */
public class ValueVectorAdapter implements ValueVector
{
    protected ValueVector wrapped;
    private int size;
    private ResolvedType type;

    public ValueVectorAdapter(ValueVector wrapped)
    {
        setVector(wrapped);
    }

    /**
     * Change wrapped vector on this adapter. NOTE! Use with care when mutating state
     */
    public void setVector(ValueVector wrapped)
    {
        this.wrapped = requireNonNull(wrapped, "wrapped");
        this.size = wrapped.size();
        this.type = wrapped.type();
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
    public boolean hasNulls()
    {
        return wrapped.hasNulls();
    }

    @Override
    public boolean isNull(int row)
    {
        return wrapped.isNull(getRow(row));
    }

    @Override
    public boolean getBoolean(int row)
    {
        return wrapped.getBoolean(getRow(row));
    }

    @Override
    public int getInt(int row)
    {
        return wrapped.getInt(getRow(row));
    }

    @Override
    public long getLong(int row)
    {
        return wrapped.getLong(getRow(row));
    }

    @Override
    public Decimal getDecimal(int row)
    {
        return wrapped.getDecimal(getRow(row));
    }

    @Override
    public float getFloat(int row)
    {
        return wrapped.getFloat(getRow(row));
    }

    @Override
    public double getDouble(int row)
    {
        return wrapped.getDouble(getRow(row));
    }

    @Override
    public Object getAny(int row)
    {
        return wrapped.getAny(getRow(row));
    }

    @Override
    public UTF8String getString(int row)
    {
        return wrapped.getString(getRow(row));
    }

    @Override
    public EpochDateTime getDateTime(int row)
    {
        return wrapped.getDateTime(getRow(row));
    }

    @Override
    public EpochDateTimeOffset getDateTimeOffset(int row)
    {
        return wrapped.getDateTimeOffset(getRow(row));
    }

    @Override
    public ObjectVector getObject(int row)
    {
        return wrapped.getObject(getRow(row));
    }

    @Override
    public ValueVector getArray(int row)
    {
        return wrapped.getArray(getRow(row));
    }

    @Override
    public TupleVector getTable(int row)
    {
        return wrapped.getTable(getRow(row));
    }

    /** Modify the row to get from the wrapped vector */
    protected int getRow(int row)
    {
        return row;
    }
}
