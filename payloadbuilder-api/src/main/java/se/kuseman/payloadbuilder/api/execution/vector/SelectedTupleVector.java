package se.kuseman.payloadbuilder.api.execution.vector;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Tuple vector that wraps an existing tuple vector along with a selection to only return specific rows.
 */
public class SelectedTupleVector implements TupleVector
{
    private final TupleVector source;
    private final ValueVector selection;
    private final ValueVector[] columns;

    private SelectedTupleVector(TupleVector source, ValueVector selection)
    {
        this.source = source;
        this.selection = selection;
        this.columns = new ValueVector[source.getSchema()
                .getSize()];
    }

    @Override
    public int getRowCount()
    {
        return selection.size();
    }

    @Override
    public ValueVector getColumn(int column)
    {
        ValueVector vector = columns[column];
        if (vector == null)
        {
            vector = SelectedValueVector.select(source.getColumn(column), selection);
            columns[column] = vector;
        }
        return vector;
    }

    @Override
    public Schema getSchema()
    {
        return source.getSchema();
    }

    /** Create a selected tuple vector from provided source and selection */
    public static TupleVector select(TupleVector vector, ValueVector selection)
    {
        requireNonNull(vector);
        requireNonNull(selection);
        return new SelectedTupleVector(vector, selection);
    }
}
