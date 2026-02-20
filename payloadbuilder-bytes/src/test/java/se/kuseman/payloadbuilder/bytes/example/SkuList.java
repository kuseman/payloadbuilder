package se.kuseman.payloadbuilder.bytes.example;

import java.util.AbstractList;
import java.util.RandomAccess;

import se.kuseman.payloadbuilder.api.execution.TupleVector;

/**
 * A real {@link java.util.List} so callers get idiomatic {@code get}/{@code size}/{@code stream}/for-each for free, backed lazily by the nested sku {@link TupleVector} - {@code get(i)} allocates a
 * single small {@link Sku} and touches no row data; only calling a getter on that {@link Sku} decodes anything, and only the one column it asked for.
 */
final class SkuList extends AbstractList<ISku> implements RandomAccess
{
    private final ColumnCache columns;

    SkuList(TupleVector tupleVector)
    {
        this.columns = new ColumnCache(tupleVector, SkuSchema.COLUMN_COUNT);
    }

    @Override
    public ISku get(int index)
    {
        if (index < 0
                || index >= size())
        {
            throw new IndexOutOfBoundsException("index: " + index + ", size: " + size());
        }
        return new Sku(columns, index);
    }

    @Override
    public int size()
    {
        return columns.rowCount();
    }
}
