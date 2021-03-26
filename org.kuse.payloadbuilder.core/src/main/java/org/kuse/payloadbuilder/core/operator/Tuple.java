package org.kuse.payloadbuilder.core.operator;

import java.util.Iterator;

import org.apache.commons.collections.IteratorUtils;

/** Result produced by an {@link Operator} */
public interface Tuple
{
    /** Returns this tuple's ordinal */
    int getTupleOrdinal();

    /**
     * Get tuple by ordinal.
     *
     * <pre>
     * If this is a composite tuple that consist of other tuples
     * this method is called to fetch the sub tuple by it's ordinal
     *
     * Ie. a query:
     *
     * select *
     * from tableA a
     * inner join tableB b
     *   on a.col = b.col
     *
     * We will get a composite tuple stream that looks like:
     *
     * CompositeTuple
     *   RowTuple (a)     (ordinal = 0)
     *   RowTuple (b)     (ordinal = 1)
     * </pre>
     *
     * @param tupleOrdinal Ordinal to resolve
     */
    Tuple getTuple(int tupleOrdinal);

    /**
     * Get value for provided column ordinal
     *
     * @param columnOrdinal Ordinal to resolve
     */
    default Object getValue(int columnOrdinal)
    {
        throw new RuntimeException();
    }

    /**
     * Get value for provided column
     *
     * @param column Column to resolve
     */
    Object getValue(String column);

    /**
     * Return an iterator with all values for provided ordinal
     * NOTE! Value returned iterator might be a singleton if implementers chooses to.
     * @param tupleOrdinal Ordinal to stream values for or -1 to stream all tuples values
     */
    default Iterator<TupleColumn> getColumns(int tupleOrdinal)
    {
        return IteratorUtils.emptyIterator();
    }

    /**
     * Definition of a tuple column. Return when streaming all columns from a tuple
     */
    interface TupleColumn
    {
        /** Which tuple ordinal this value belongs to. */
        int getTupleOrdinal();

        /** Name of the column */
        String getColumn();
    }

    /** Marker interface for a tuple which contains computed values  */
    interface ComputedTuple extends Tuple
    {
        /** Get computed value for provided ordinal */
        Object getComputedValue(int ordinal);
    }
}
