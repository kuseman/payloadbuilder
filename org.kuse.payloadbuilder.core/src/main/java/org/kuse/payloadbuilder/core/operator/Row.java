package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.catalog.TableMeta.Column;

/**
 * Row implementation of a {@link Tuple}. Columns and values simply
 */
public class Row implements Tuple
{
    private static final String POS = "__pos";
    static final int POS_ORDINAL = Integer.MAX_VALUE;

    private final int pos;
    final String[] columns;
    final TableAlias tableAlias;
    private final RowValues values;

    /** Array used if this row gets optimized */
    final Object[] optimizedValues;

    Row(TableAlias alias, int pos, String[] columns, RowValues values)
    {
        this.tableAlias = requireNonNull(alias, "alias");
        this.pos = pos;
        this.columns = requireNonNull(columns, "columns");
        this.values = requireNonNull(values);
        this.optimizedValues = null;
    }

    /** Columns from table meta in table alias */
    Row(TableAlias alias, int pos, RowValues values)
    {
        this.tableAlias = requireNonNull(alias, "alias");
        this.pos = pos;
        this.columns = null;
        this.values = requireNonNull(values);
        this.optimizedValues = null;
    }

    Row(TableAlias alias, int pos, String[] columns, Object[] optimizedValues)
    {
        this.tableAlias = requireNonNull(alias, "alias");
        this.pos = pos;
        this.columns = columns;
        this.values = null;
        this.optimizedValues = requireNonNull(optimizedValues);
    }

    @Override
    public int getTupleOrdinal()
    {
        return tableAlias.getTupleOrdinal();
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
        if (ordinal <= tableAlias.getTupleOrdinal())
        {
            return this;
        }
        return null;
    }

    @Override
    public int getColumnCount()
    {
        return columns != null ? columns.length : tableAlias.getTableMeta().getColumns().size();
    }

    @Override
    public int getColumnOrdinal(String column)
    {
        if (POS.equals(column))
        {
            return POS_ORDINAL;
        }

        if (columns != null)
        {
            return ArrayUtils.indexOf(columns, column);
        }

        Column col = tableAlias.getTableMeta().getColumn(column);
        return col != null ? col.getOrdinal() : -1;
    }

    @Override
    public String getColumn(int ordinal)
    {
        if (columns != null)
        {
            return columns[ordinal];
        }

        Column col = tableAlias.getTableMeta().getColumn(ordinal);
        return col != null ? col.getName() : null;
    }

    @Override
    public Object getValue(int columnOrdinal)
    {
        if (columnOrdinal < 0)
        {
            return null;
        }
        else if (columnOrdinal == POS_ORDINAL)
        {
            return pos;
        }
        else if (optimizedValues != null)
        {
            return optimizedValues[columnOrdinal];
        }

        return values.getValue(columnOrdinal);
    }

    @Override
    public Tuple optimize(ExecutionContext context)
    {
        if (optimizedValues != null)
        {
            return this;
        }

        Object[] optimizedValues;
        if (!(values instanceof ObjectValues))
        {
            int length = columns.length;
            optimizedValues = new Object[length];
            for (int i = 0; i < length; i++)
            {
                optimizedValues[i] = this.values.getValue(i);
            }
        }
        else
        {
            optimizedValues = ((ObjectValues) values).values;
        }

        // Intern the values
        context.intern(optimizedValues);
        return new Row(tableAlias, pos, columns, optimizedValues);
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
                && tableAlias.equals(that.tableAlias);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return tableAlias + ", pos: " + pos;
    }

    /** {@link Row#of(TableAlias, int, String[], RowValues)}} */
    public static Row of(TableAlias alias, int pos, Object[] values)
    {
        return of(alias, pos, new ObjectValues(values));
    }

    /** {@link Row#of(TableAlias, int, String[], RowValues)}} */
    public static Row of(TableAlias alias, int pos, RowValues values)
    {
        if (alias.getTableMeta() == null)
        {
            throw new IllegalArgumentException("Cannot construct a Row without columns.");
        }

        return new Row(alias, pos, values);
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
            if (ordinal >= values.length)
            {
                System.out.println();
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
