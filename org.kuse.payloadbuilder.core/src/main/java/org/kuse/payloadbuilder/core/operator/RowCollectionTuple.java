package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;

/**
 * Optimized variant of {@link CollectionTuple}
 *
 * <pre>
 * If the collection tuples all child tuples was of Row type with the same set of columns
 * it's converted to a column store instead of a row store. This to reduce the amount of allocated Object-arrays.
 *
 * ie.
 *
 * CollectionTuple
 *   Row(1)
 *   Row(2)
 *   Row(3)
 *
 * will be converted to:
 *
 * RowCollectionTuple
 *   String[] columns
 *   List<Object[]> columnValues
 * </pre>
 */
class RowCollectionTuple implements Tuple, Iterable<Tuple>
{
    private final TableAlias tableAlias;
    private final String[] columns;
    /** List of object arrays with values for each column */
    private final List<Object[]> columnValues;
    private final Object[] row;
    private final int rowSize;

    RowCollectionTuple(TableAlias tableAlias, String[] columns, int rowSize, List<Object[]> columnValues, Object[] row)
    {
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.columns = requireNonNull(columns, "columns");
        this.columnValues = columnValues;
        this.row = row;
        if (columnValues == null && row == null)
        {
            throw new IllegalArgumentException("Both row and rows cannot be null");
        }
        this.rowSize = rowSize;
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        if (row != null)
        {
            return IteratorUtils.singletonIterator(new Row(tableAlias, 0, columns, row));
        }
        // CSOFF
        return new Iterator<Tuple>()
        // CSON
        {
            int index;

            @Override
            public Tuple next()
            {
                // Extract all column values for current row index
                int length = columns.length;
                Object[] values = new Object[length];
                for (int i = 0; i < length; i++)
                {
                    // Current columns values
                    Object[] vals = columnValues.get(i);

                    // Same column value array, then pick index 0
                    if (index >= vals.length)
                    {
                        values[i] = vals[0];
                    }
                    else
                    {
                        values[i] = vals[index];
                    }
                }

                return new Row(tableAlias, index++, columns, values);
            }

            @Override
            public boolean hasNext()
            {
                return index < rowSize;
            }
        };
    }

    @Override
    public int getTupleOrdinal()
    {
        return tableAlias.getTupleOrdinal();
    }

    @Override
    public int getColumnCount()
    {
        return columns.length;
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return ArrayUtils.indexOf(columns, column);
    }

    @Override
    public String getColumn(int columnOrdinal)
    {
        return columns[columnOrdinal];
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        if (columnOrdinal < 0)
        {
            return null;
        }
        if (row != null)
        {
            return row[columnOrdinal];
        }

        return columnValues.get(columnOrdinal)[0];
    }
}
