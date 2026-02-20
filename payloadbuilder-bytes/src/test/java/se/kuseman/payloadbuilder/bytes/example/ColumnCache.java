package se.kuseman.payloadbuilder.bytes.example;

import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Decodes each column of a {@link TupleVector} at most once, sharing the result across every row-wrapper built from it.
 *
 * <p>
 * This matters because {@code TupleVector#getColumn(int)} re-decodes that column from scratch on every call - a row-wrapper (eg. {@link Sku}) that called it directly on every field access would, when
 * iterated over many rows, redundantly re-parse the entire column once per row instead of once total. Caching it here, at the container level, means the cost is paid exactly once no matter how many
 * row-wrappers end up reading it.
 * </p>
 */
final class ColumnCache
{
    private final TupleVector tupleVector;
    private final ValueVector[] cache;

    ColumnCache(TupleVector tupleVector, int columnCount)
    {
        this.tupleVector = tupleVector;
        this.cache = new ValueVector[columnCount];
    }

    ValueVector column(int ordinal)
    {
        ValueVector v = cache[ordinal];
        if (v == null)
        {
            v = tupleVector.getColumn(ordinal);
            cache[ordinal] = v;
        }
        return v;
    }

    int rowCount()
    {
        return tupleVector.getRowCount();
    }
}
