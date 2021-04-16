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

    /**
     * Get sub tuple by ordinal
     *
     * <pre>
     * Special kinds of tuples (like temporary table tuples)
     * has the target tuple hierarchy embedded inside it self
     * and this method resolve a tuple within the the target alias
     * hierarchy.
     *
     *  Ie.
     *
     *  select *
     *  into #temp
     *  from tableA a
     *  inner join tableB b with (populate=true)
     *    on b.col = c.col
     *
     *  select (select col from open_rows(t.b) for array)
     *  from #temp t
     *
     *  Here we resolve INTO the temp tables sub alias (tableB)
     *  The expression 't.b' like this:
     * </pre>
     *
     * @param tupleOrdinal The sub tuple ordinal to resolve
     */
    default Tuple getSubTuple(int tupleOrdinal)
    {
        throw new IllegalArgumentException(getClass().getSimpleName() + " does not support sub tuples.");
    }

    /** Return the number of columns that this tuple has */
    int getColumnCount();

    /** Get the ordinal of provided column. */
    int getColumnOrdinal(String column);

    /** Get the column name of the provided ordinal */
    String getColumn(int ordinal);

    /** Get value for provided column ordinal */
    Object getValue(int ordinal);
}
