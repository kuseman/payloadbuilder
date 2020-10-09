package org.kuse.payloadbuilder.core.operator;

import static org.apache.commons.lang3.StringUtils.equalsAnyIgnoreCase;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.utils.MapUtils;

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
    public String[] getColumns()
    {
        return columns != null ? columns : tableAlias.getColumns();
    }

    private int getColumnCount()
    {
        return getColumns().length;
    }

    /** Extracts values into an object array */
    public Object[] getValues()
    {
        if (values instanceof ObjectValues)
        {
            return ((ObjectValues) values).values;
        }
        Object[] values = new Object[getColumnCount()];
        for (int i = 0; i < values.length; i++)
        {
            values[i] = getObject(i);
        }
        return values;
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        int size = qname.getParts().size();

        // First part is pointing to this alias, step up one part
        int index = partIndex;
        if (size - 1 > index && equalsAnyIgnoreCase(qname.getParts().get(index), tableAlias.getAlias()))
        {
            index++;
        }

        Object result = getObject(qname.getParts().get(index));

        if (result == null)
        {
            return null;
        }

        if (index < size - 1)
        {
            if (result instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) result;
                return MapUtils.traverse(map, qname.getParts().subList(index + 1, size));
            }

            throw new IllegalArgumentException("Cannot dereference value " + result);
        }

        return result;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return equalsIgnoreCase(alias, tableAlias.getAlias());
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        String[] columns = getColumns();
        String alias = tableAlias.getAlias();
        return Arrays.stream(columns)
                .map(c -> isNotBlank(alias) ? QualifiedName.of(alias, c) : QualifiedName.of(c))
                .iterator();
    }

    /** Get value for provided ordinal */
    public Object getObject(int ordinal)
    {
        if (ordinal < 0)
        {
            return null;
        }
        return values.get(ordinal);
    }

    /** Get value for provided column */
    public Object getObject(String column)
    {
        if (POS.equals(column))
        {
            return pos;
        }
        int ordinal = ArrayUtils.indexOf(getColumns(), column);
        return getObject(ordinal);
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
        return tableAlias.getTable() + " (" + pos + ") " + values;
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
