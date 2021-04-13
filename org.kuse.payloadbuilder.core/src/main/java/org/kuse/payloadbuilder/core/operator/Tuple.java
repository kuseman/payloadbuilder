package org.kuse.payloadbuilder.core.operator;

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

    /** Return the number of columns that this tuple has */
    int getColumnCount();

    /** Get the ordinal of provided column. */
    int getColmnOrdinal(String column);

    /** Get the column name of the provided ordinal */
    String getColumn(int ordinal);

    /** Get value for provided column ordinal */
    Object getValue(int ordinal);
}
