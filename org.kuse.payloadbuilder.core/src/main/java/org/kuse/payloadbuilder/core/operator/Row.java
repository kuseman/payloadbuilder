package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/**
 * Row implementation of a {@link Tuple}. Columns and values simply
 */
public class Row implements Tuple
{
    private static final String POS = "__pos";
    static final int POS_ORDINAL = Integer.MAX_VALUE;

    private final int pos;
    private final QualifiedName table;
    private final int tupleOrdinal;
    private final String[] columns;
    private final RowValues values;

    private Row(TableAlias alias, int pos, String[] columns, RowValues values)
    {
        this.table = requireNonNull(alias, "alias").getTable();
        this.tupleOrdinal = alias.getTupleOrdinal();
        this.pos = pos;
        this.columns = requireNonNull(columns, "columns");
        this.values = requireNonNull(values);
    }

    @Override
    public int getTupleOrdinal()
    {
        return tupleOrdinal;
    }

    @Override
    public Tuple getTuple(int ordinal)
    {
        // If the wanted tuple ordinal is a parent of this row
        // then return our selves.
        // This means we have a tuple stream of only rows
        // but wrapped in a sub query etc. so the ordinal will be one level up
        /*
         * ie
         *
         * select x.col1,               <- target ordinal 0
         *        x.col2                <- target ordinal 0
         * from                         <- ordinal 0
         * (
         *   from table                 <- ordinal 1
         * ) x
         *
         */
        if (ordinal <= tupleOrdinal)
        {
            return this;
        }
        return null;
    }

    @Override
    public int getColumnCount()
    {
        return columns.length;
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        if (POS.equalsIgnoreCase(column))
        {
            return POS_ORDINAL;
        }

        return ArrayUtils.indexOf(columns, column);
    }

    @Override
    public String getColumn(int ordinal)
    {
        return ordinal < columns.length ? columns[ordinal] : null;
    }

    @Override
    public Object getValue(int ordinal)
    {
        if (ordinal == POS_ORDINAL)
        {
            return pos;
        }

        return values.getValue(ordinal);
    }

    @Override
    public int hashCode()
    {
        return pos;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Row)
        {
            Row that = (Row) obj;
            return pos == that.pos
                && table.equals(that.table)
                && tupleOrdinal == that.tupleOrdinal;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return table + "(" + tupleOrdinal + ") pos: " + pos;
    }

    /** {@link Row#of(TableAlias, int, String[], RowValues)}} */
    public static Row of(TableAlias alias, int pos, Object[] values)
    {
        return of(alias, pos, alias.getColumns(), values);
    }

    /** {@link Row#of(TableAlias, int, String[], RowValues)}} */
    public static Row of(TableAlias alias, int pos, String[] columns, Object[] values)
    {
        return of(alias, pos, columns, new ObjectValues(values));
    }

    /**
     * Construct a row with provided alias, columns, values and position
     *
     * <pre>
     * NOTE! Row of the same table alias type must have all their columns
     * in the same order. These are accessed by ordinal and cached for faster
     * access.
     *
     * However it's not a requirement that all rows has the same amount of columns
     * so the following is valid
     * Ie.
     *
     * Row1: col1, col2
     * Row2: col1, col2, col3
     * Row3: col1
     *
     * etc.
     * </pre>
     */
    public static Row of(TableAlias alias, int pos, String[] columns, RowValues values)
    {
        return new Row(alias, pos, columns, values);
    }

    /**
     * RowValues
     *
     * <pre>
     * This is to delegate the value handing to operators if they can improve the performance etc.
     * Instead of sending in an Object-array
     * </pre>
     */
    public interface RowValues
    {
        /** Get value for provided ordinal */
        Object getValue(int ordinal);
    }

    /** Implementation of {@link RowValues} that wraps a Map */
    public static class MapValues implements Row.RowValues
    {
        protected final Map<String, Object> map;
        protected final String[] columns;

        public MapValues(Map<String, Object> map, String[] columns)
        {
            this.map = map;
            this.columns = columns;
        }

        @Override
        public Object getValue(int ordinal)
        {
            return map.get(columns[ordinal]);
        }
    }

    /** Object array implementation of {@link RowValues} */
    private static class ObjectValues implements RowValues
    {
        private final Object[] values;

        ObjectValues(Object[] values)
        {
            this.values = values;
        }

        @Override
        public Object getValue(int ordinal)
        {
            if (ordinal < 0 || ordinal >= values.length)
            {
                return null;
            }

            return values[ordinal];
        }

        @Override
        public String toString()
        {
            return Arrays.toString(values);
        }
    }
}
