package org.kuse.payloadbuilder.core.operator;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.kuse.payloadbuilder.core.operator.Operator.RowList;

/**
 * Tuple that is composed of tuples of the same table source in a collection. type fashion. Used by populating joins
 **/
class CollectionTuple extends AbstractList<Tuple> implements Tuple, RowList
{
    private static final int DEFAULT_CAPACITY = 10;
    private final int tupleOrdinal;
    /** Start reference, if the size increases then a list will be created instead #tuples */
    private Tuple tuple;
    /** The list of tuples that this collection consists of */
    private List<Tuple> tuples;

    /**
     * Create a collection tuple based on another tuple
     *
     * @param tuple Initial tuple in this collection
     * @param tupleOrdinal
     *
     *            <pre>
     * The ordinal of this collection.
     * If this is a sub query this tuple ordinal is the parent of ordinal of the tuples in this collection
     *            </pre>
     */
    CollectionTuple(Tuple tuple, int tupleOrdinal)
    {
        this.tuple = tuple;
        this.tupleOrdinal = tupleOrdinal;
    }

    /** Return ev. single tuple */
    Tuple getSingleTuple()
    {
        if (tuple != null)
        {
            return tuple;
        }

        return null;
    }

    /** Add tuple to this collection */
    void addTuple(Tuple tuple)
    {
        if (tuples == null)
        {
            // Switch to array list
            this.tuples = new ArrayList<>(DEFAULT_CAPACITY);
            this.tuples.add(this.tuple);
            this.tuples.add(tuple);
            this.tuple = null;
        }
        else
        {
            tuples.add(tuple);
        }
    }

    /* (Row)List implementation */

    @Override
    public int size()
    {
        return tuple != null ? 1 : tuples.size();
    }

    @Override
    public Tuple get(int index)
    {
        return tuple != null ? tuple : tuples.get(index);
    }

    /* (Row)List implementation */

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public int getColumnCount()
    {
        return getFirstTuple().getColumnCount();
    }

    @Override
    public String getColumn(int columnOrdinal)
    {
        return getFirstTuple().getColumn(columnOrdinal);
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        return getFirstTuple().getColumnOrdinal(column);
    }

    @Override
    public Tuple getTuple(int tupleOrdinal)
    {
        return getFirstTuple().getTuple(tupleOrdinal);
    }

    @Override
    public Object getValue(int ordinal)
    {
        return getFirstTuple().getValue(ordinal);
    }

    @Override
    public int getInt(int ordinal)
    {
        return getFirstTuple().getInt(ordinal);
    }

    @Override
    public long getLong(int ordinal)
    {
        return getFirstTuple().getLong(ordinal);
    }

    @Override
    public float getFloat(int ordinal)
    {
        return getFirstTuple().getFloat(ordinal);
    }

    @Override
    public double getDouble(int ordinal)
    {
        return getFirstTuple().getDouble(ordinal);
    }

    @Override
    public boolean getBool(int ordinal)
    {
        return getFirstTuple().getBool(ordinal);
    }

    @Override
    public Tuple optimize(ExecutionContext context)
    {
        if (tuples instanceof ArrayList)
        {
            ((ArrayList<Tuple>) tuples).trimToSize();
        }

        boolean pullUpRows = false;
        String[] columns = null;

        if (tuple != null)
        {
            tuple = tuple.optimize(context);
            if (tuple instanceof Row)
            {
                pullUpRows = true;
                columns = ((Row) tuple).columns;
            }
        }
        else
        {
            int size = tuples.size();
            for (int i = 0; i < size; i++)
            {
                Tuple tuple = tuples.get(i);
                tuples.set(i, tuple.optimize(context));

                // Check to see if all children are rows with the same columns
                // then we can pull up all rows to a more memory efficient structure
                if (tuple instanceof Row)
                {
                    Row row = (Row) tuple;
                    //CSOFF
                    if (columns == null)
                    //CSON
                    {
                        pullUpRows = true;
                        columns = row.columns;
                    }
                    else if (row.columns != columns)
                    {
                        pullUpRows = false;
                    }
                }
            }
        }

        // Convert to column store if possible
        if (pullUpRows)
        {
            return pullUpRows(columns);
        }

        return this;
    }

    private Tuple pullUpRows(String[] columns)
    {
        if (tuple != null)
        {
            Row row = (Row) tuple;
            return new RowCollectionTuple(row.tableAlias, columns, 1, null, row.optimizedValues);
        }
        else
        {
            int columnSize = columns.length;
            int rowSize = tuples.size();
            List<Object[]> columnValues = new ArrayList<>(columnSize);
            for (int i = 0; i < columnSize; i++)
            {
                Object[] values = new Object[rowSize];
                Object prevValue = null;
                boolean allSame = true;
                for (int j = 0; j < rowSize; j++)
                {
                    Object value = ((Row) tuples.get(j)).optimizedValues[i];
                    values[j] = value;
                    if (prevValue == null)
                    {
                        prevValue = value;
                    }
                    else if (prevValue != value)
                    {
                        allSame = false;
                    }
                }

                // If all the values are the same in the array then only use the first value
                if (allSame)
                {
                    values = new Object[] {values[0]};
                }
                columnValues.add(values);
            }
            Row row = (Row) tuples.get(0);
            return new RowCollectionTuple(row.tableAlias, columns, rowSize, columnValues, null);
        }
    }

    private Tuple getFirstTuple()
    {
        if (tuple != null)
        {
            return tuple;
        }
        return tuples.get(0);
    }
}
