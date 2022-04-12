package se.kuseman.payloadbuilder.core.operator;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.operator.Operator.TupleList;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/**
 * Tuple that is composed of tuples of the same table source in a collection. type fashion. Used by populating joins
 **/
class CollectionTuple extends AbstractList<Tuple> implements Tuple, TupleList
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
     * <pre>
     * The ordinal of this collection.
     * If this is a sub query this tuple ordinal is the parent of ordinal of the tuples in this collection
     * </pre>
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
        return tuple != null ? 1
                : tuples.size();
    }

    @Override
    public Tuple get(int index)
    {
        return tuple != null ? tuple
                : tuples.get(index);
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

    private Tuple getFirstTuple()
    {
        if (tuple != null)
        {
            return tuple;
        }
        return tuples.get(0);
    }
}
