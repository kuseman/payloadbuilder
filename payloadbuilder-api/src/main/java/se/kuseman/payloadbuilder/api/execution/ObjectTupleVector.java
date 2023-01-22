package se.kuseman.payloadbuilder.api.execution;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/**
 * Convenience class for creating tuple vector in an easy reflective way. NOTE! This class should not be used if high performance catalogs is needed since all values here is auto boxed
 */
public class ObjectTupleVector implements TupleVector
{
    private final Schema schema;
    private final ValueProvider valueProvider;
    private final int rowCount;

    /**
     * Create a tuple vector with provided schema and row count. The value provided extracts values in a column/row index fashion.
     */
    public ObjectTupleVector(Schema schema, int rowCount, ValueProvider valueProvider)
    {
        this.schema = requireNonNull(schema, "schema");
        this.rowCount = rowCount;
        this.valueProvider = requireNonNull(valueProvider, "valueProvider");
    }

    @Override
    public int getRowCount()
    {
        return rowCount;
    }

    @Override
    public ValueVector getColumn(int column)
    {
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return schema.getColumns()
                        .get(column)
                        .getType();
            }

            @Override
            public int size()
            {
                return rowCount;
            }

            @Override
            public boolean isNull(int row)
            {
                return valueProvider.getValue(row, column) == null;
            }

            @Override
            public Object getAny(int row)
            {
                return valueProvider.getValue(row, column);
            }

            @Override
            public ValueVector getArray(int row)
            {
                return (ValueVector) getAny(row);
            }
        };
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    /** Value provider for tuple vector */
    @FunctionalInterface
    public interface ValueProvider
    {
        Object getValue(int row, int col);
    }
}
