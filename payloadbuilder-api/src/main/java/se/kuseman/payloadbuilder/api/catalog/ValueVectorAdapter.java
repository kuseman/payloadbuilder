package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.function.IntFunction;

/** Decorator for {@link ValueVector} used to remove boiler plate */
public class ValueVectorAdapter implements ValueVector
{
    protected final ValueVector wrapped;
    private final int size;
    private final boolean nullable;
    private final ResolvedType type;

    private final IntFunction<ValueVector> supplier;

    public ValueVectorAdapter(ValueVector wrapped)
    {
        this.wrapped = requireNonNull(wrapped, "wrapped");
        this.supplier = null;
        this.size = wrapped.size();
        this.nullable = wrapped.isNullable();
        this.type = wrapped.type();
    }

    public ValueVectorAdapter(IntFunction<ValueVector> supplier, int size, boolean nullable, ResolvedType type)
    {
        this.wrapped = null;
        this.supplier = requireNonNull(supplier, "supplier");
        this.size = size;
        this.nullable = nullable;
        this.type = type;
    }

    private ValueVector get(int row)
    {
        if (wrapped != null)
        {
            return wrapped;
        }
        return supplier.apply(row);
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
    public boolean isNullable()
    {
        return nullable;
    }

    @Override
    public boolean isNull(int row)
    {
        return get(row).isNull(getRow(row));
    }

    @Override
    public boolean getBoolean(int row)
    {
        return get(row).getBoolean(getRow(row));
    }

    @Override
    public int getInt(int row)
    {
        return get(row).getInt(getRow(row));
    }

    @Override
    public long getLong(int row)
    {
        return get(row).getLong(getRow(row));
    }

    @Override
    public float getFloat(int row)
    {
        return get(row).getFloat(getRow(row));
    }

    @Override
    public double getDouble(int row)
    {
        return get(row).getDouble(getRow(row));
    }

    @Override
    public Object getValue(int row)
    {
        return get(row).getValue(getRow(row));
    }

    @Override
    public UTF8String getString(int row)
    {
        return get(row).getString(getRow(row));
    }

    @Override
    public EpochDateTime getDateTime(int row)
    {
        return get(row).getDateTime(getRow(row));
    }

    /** Modify the row to get from the wrapped vector */
    protected int getRow(int row)
    {
        return row;
    }
}
