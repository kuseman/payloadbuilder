package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Wraps a {@link TupleVector} adding a {@code __coverage__} string column containing coverage JSON. */
class CoverageTupleVector implements TupleVector
{
    static final String COVERAGE_COLUMN = "__coverage__";

    private final TupleVector delegate;
    private final ValueVector coverageColumn;
    private Schema schema;

    CoverageTupleVector(TupleVector delegate, String coverageJson)
    {
        this.delegate = requireNonNull(delegate, "delegate");
        this.coverageColumn = ValueVector.literalString(coverageJson, delegate.getRowCount());
    }

    @Override
    public int getRowCount()
    {
        return delegate.getRowCount();
    }

    @Override
    public ValueVector getColumn(int column)
    {
        int delegateSize = delegate.getSchema()
                .getSize();
        if (column < delegateSize)
        {
            return delegate.getColumn(column);
        }
        return coverageColumn;
    }

    @Override
    public Schema getSchema()
    {
        if (schema == null)
        {
            schema = buildSchema();
        }
        return schema;
    }

    private Schema buildSchema()
    {
        List<Column> columns = new ArrayList<>(delegate.getSchema()
                .getColumns());
        columns.add(Column.of(COVERAGE_COLUMN, Type.String));
        return Schema.of(columns.toArray(new Column[0]));
    }
}
