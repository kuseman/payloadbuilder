package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.function.BiFunction;

/**
 * Convenience class for creating tuple vector in an easy reflective way. NOTE! This class should not be used if high performance catalogs is needed since all values here is auto boxed
 */
public class ObjectTupleVector implements TupleVector
{
    private final Schema schema;
    private final BiFunction<Integer, Integer, Object> valueProvider;
    private final int rowCount;

    /**
     * Create a tuple vector with provided schema and row count. The value provided extracts values in a column/row index fashion.
     */
    public ObjectTupleVector(Schema schema, int rowCount, BiFunction<Integer, Integer, Object> valueProvider)
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
                return valueProvider.apply(row, column) == null;
            }

            @Override
            public Object getValue(int row)
            {
                return valueProvider.apply(row, column);
            }
        };
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }
}
