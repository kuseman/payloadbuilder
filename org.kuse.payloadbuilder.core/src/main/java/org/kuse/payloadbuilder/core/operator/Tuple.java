package org.kuse.payloadbuilder.core.operator;

/**
 * Result produced by an {@link Operator}
 */
public interface Tuple
{
    /** Returns this tuple's ordinal */
    int getTupleOrdinal();

    /**
     * Get the ordinal of provided column.
     *
     * @param column Column to fetch ordinal for
     */
    int getColumnOrdinal(String column);

    /**
     * Get tuple by ordinal.
     *
     * <pre>
     * Return a tuple value with provided tupleOrdinal.
     * This means that we want to access an alias and not a column
     * in the table alias hierarchy
     * </pre>
     *
     * @param tupleOrdinal Ordinal to resolve value for.
     */
    default Tuple getTuple(int tupleOrdinal)
    {
        if (tupleOrdinal == getTupleOrdinal())
        {
            return this;
        }

        return null;
    }

    /**
     * Get value for provided column ordinal
     *
     * @param columnOrdinal The ordinal of the column to fetch
     */
    Object getValue(int columnOrdinal);

    /**
     * Returns true if provided column ordinal is null otherwise false
     *
     * @param columnOrdinal Column ordinal to get null for.
     */
    default boolean isNull(int columnOrdinal)
    {
        return getValue(columnOrdinal) == null;
    }

    /** Get int value from provided ordinals */
    default int getInt(int columnOrdinal)
    {
        return (int) getValue(columnOrdinal);
    }

    /** Get long value from provided ordinals */
    default long getLong(int columnOrdinal)
    {
        return (long) getValue(columnOrdinal);
    }

    /** Get float value from provided ordinals */
    default float getFloat(int columnOrdinal)
    {
        return (float) getValue(columnOrdinal);
    }

    /** Get double value from provided ordinals */
    default double getDouble(int columnOrdinal)
    {
        return (double) getValue(columnOrdinal);
    }

    /** Get bool value from provided ordinals */
    default boolean getBool(int columnOrdinal)
    {
        return (boolean) getValue(columnOrdinal);
    }

    /**
     * Get the column name of the provided ordinal
     *
     * @param columnOrdinal Ordinal of column to return
     */
    default String getColumn(int columnOrdinal)
    {
        return null;
    }

    /**
     * Return the number of columns that this tuple has.
     */
    default int getColumnCount()
    {
        return 0;
    }

    /**
     * Optimize this tuple. Shrink resources etc.
     *
     * <pre>
     * This method is called before the tuple is put to cache to optimize
     * array sizes etc.
     * </pre>
     *
     * @param context Execution context
     * @return A optimized tuple
     */
    default Tuple optimize(ExecutionContext context)
    {
        return this;
    }
}
