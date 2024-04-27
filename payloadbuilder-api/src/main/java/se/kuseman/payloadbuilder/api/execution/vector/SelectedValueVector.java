package se.kuseman.payloadbuilder.api.execution.vector;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Vector that wraps another vector with a selection and returns only those rows */
public class SelectedValueVector implements ValueVector
{
    private final ValueVector vector;
    private final ValueVector selection;

    private SelectedValueVector(ValueVector vector, ValueVector selection)
    {
        this.vector = requireNonNull(vector, "vector");
        this.selection = requireNonNull(selection, "selection");
    }

    @Override
    public ResolvedType type()
    {
        return vector.type();
    }

    @Override
    public int size()
    {
        return selection.size();
    }

    @Override
    public boolean isNull(int row)
    {
        return vector.isNull(selection.getInt(row));
    }

    @Override
    public boolean getBoolean(int row)
    {
        return vector.getBoolean(selection.getInt(row));
    }

    @Override
    public int getInt(int row)
    {
        return vector.getInt(selection.getInt(row));
    }

    @Override
    public long getLong(int row)
    {
        return vector.getLong(selection.getInt(row));
    }

    @Override
    public float getFloat(int row)
    {
        return vector.getFloat(selection.getInt(row));
    }

    @Override
    public double getDouble(int row)
    {
        return vector.getDouble(selection.getInt(row));
    }

    @Override
    public Decimal getDecimal(int row)
    {
        return vector.getDecimal(selection.getInt(row));
    }

    @Override
    public UTF8String getString(int row)
    {
        return vector.getString(selection.getInt(row));
    }

    @Override
    public EpochDateTime getDateTime(int row)
    {
        return vector.getDateTime(selection.getInt(row));
    }

    @Override
    public EpochDateTimeOffset getDateTimeOffset(int row)
    {
        return vector.getDateTimeOffset(selection.getInt(row));
    }

    @Override
    public ObjectVector getObject(int row)
    {
        return vector.getObject(selection.getInt(row));
    }

    @Override
    public ValueVector getArray(int row)
    {
        return vector.getArray(selection.getInt(row));
    }

    @Override
    public TupleVector getTable(int row)
    {
        return vector.getTable(selection.getInt(row));
    }

    @Override
    public Object getAny(int row)
    {
        return vector.getAny(selection.getInt(row));
    }

    /**
     * Create a selected value vector from provided source and selection.
     *
     * @param source Source value vector
     * @param selection Integer value vector with rows
     */
    public static ValueVector select(ValueVector source, ValueVector selection)
    {
        return new SelectedValueVector(source, selection);
    }
}
