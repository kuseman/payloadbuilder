package org.kuse.payloadbuilder.core.operator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.lang3.ArrayUtils;

/** Row */
public class Row implements Tuple
{
    private static final String POS = "__pos";

    private int pos;
    private TableAlias tableAlias;
    private String[] columns;
    private Values values;

    private Row()
    {
    }

    /** Return columns for this row */
    private String[] getColumns()
    {
        return columns != null ? columns : tableAlias.getColumns();
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
    public Object getValue(int ordinal)
    {
        return values.get(ordinal);
    }

    @Override
    public Object getValue(String column)
    {
        if (POS.equals(column))
        {
            return pos;
        }
        int ordinal = ArrayUtils.indexOf(getColumns(), column);
        if (ordinal == -1)
        {
            return null;
        }
        return values.get(ordinal);
    }

    @Override
    public Iterator<TupleColumn> getColumns(int tupleOrdinal)
    {
        final String[] columns = getColumns();
        final int length = columns.length;
        final RowTupleColumn value = new RowTupleColumn(tableAlias.getTupleOrdinal());
        //CSOFF
        return new Iterator<TupleColumn>()
        //CSON
        {
            int index;

            @Override
            public boolean hasNext()
            {
                return index < length;
            }

            @Override
            public TupleColumn next()
            {
                if (index >= length)
                {
                    throw new NoSuchElementException();
                }

                value.column = columns[index];
                index++;
                return value;
            }
        };
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
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
                && tableAlias == that.tableAlias;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return tableAlias.getTable() + " row: " + pos;
    }

    /** Row tuple value */
    private static class RowTupleColumn implements TupleColumn
    {
        private final int tupleOrdinal;
        private String column;

        RowTupleColumn(int tupleOrdinal)
        {
            this.tupleOrdinal = tupleOrdinal;
        }

        @Override
        public int getTupleOrdinal()
        {
            return tupleOrdinal;
        }

        @Override
        public String getColumn()
        {
            return column;
        }
    }

    /** Construct a row with provided alias, values and position */
    public static Row of(TableAlias alias, int pos, Object[] values)
    {
        return of(alias, pos, alias.getColumns(), values);
    }

    /** Construct a row with provided alias, columns, values and position */
    public static Row of(TableAlias alias, int pos, String[] columns, Object[] values)
    {
        return of(alias, pos, columns, new ObjectValues(values));
    }

    /** Construct a row with provided alias, values and position */
    public static Row of(TableAlias alias, int pos, Values values)
    {
        return of(alias, pos, alias.getColumns(), values);
    }

    /** Construct a row with provided parent, alias, columns, values and position */
    public static Row of(TableAlias alias, int pos, String[] columns, Values values)
    {
        Row t = new Row();
        t.pos = pos;
        t.tableAlias = alias;
        t.columns = columns;
        t.values = values;
        return t;
    }

    /** Values definition of a rows values */
    public interface Values
    {
        /** Get value for provided ordinal */
        Object get(int ordinal);
    }

    /** Implementation of {@link Values} that wraps a Map */
    public static class MapValues implements Row.Values
    {
        protected final Map<String, Object> map;
        protected final String[] columns;

        public MapValues(Map<String, Object> map, String[] columns)
        {
            this.map = map;
            this.columns = columns;
        }

        @Override
        public Object get(int ordinal)
        {
            return map.get(columns[ordinal]);
        }
    }

    /** Object array implementation of {@link Values} */
    private static class ObjectValues implements Values
    {
        private final Object[] values;

        ObjectValues(Object[] values)
        {
            this.values = values;
        }

        @Override
        public Object get(int ordinal)
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
