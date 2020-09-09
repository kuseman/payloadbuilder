package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.catalog.TableAlias;

/** Row */
public class Row
{
    protected int pos;
    // If this is a tuple row then subPos is the other rows position
    private int subPos;
    protected TableAlias tableAlias;

    /** Collection of parents that this row belongs to */
    private List<Row> parents;

    /** Temporary parent that is set during predicate evaluations (ie. join conditions) */
    private Row predicateParent;

    private Values values;
    protected List<Row>[] childRows;

    /** Temporary fields used by physical operators during join */
    public boolean match;
    public int hash;
    public Object[] extractedValues;

    Row()
    {
    }

    public Row(Row source, int subPos)
    {
        this.pos = source.pos;
        this.subPos = subPos;
        this.tableAlias = source.tableAlias;
        this.values = source.values;
        this.childRows = copyChildRows(source);
        this.parents = source.parents;
    }

    private List<Row>[] copyChildRows(Row source)
    {
        if (source.childRows == null)
        {
            return null;
        }

        @SuppressWarnings("unchecked")
        List<Row>[] copy = new List[source.childRows.length];
        int index = 0;
        for (List<Row> rows : source.childRows)
        {
            copy[index++] = rows != null ? new ArrayList<>(rows) : null;
        }

        return copy;
    }

    public int getColumnCount()
    {
        return tableAlias.getColumns() != null ? tableAlias.getColumns().length : 0;
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

    public Object getObject(int ordinal)
    {
        if (ordinal < 0)
        {
            return null;
        }
        return values.get(ordinal);
    }

    public Object getObject(String column)
    {
        int ordinal = ArrayUtils.indexOf(tableAlias.getColumns(), column);
        return getObject(ordinal);
    }

    public Number getNumber(String column)
    {
        Object obj = getObject(column);
        if (obj instanceof Number)
        {
            return (Number) obj;
        }

        return null;
    }

    public Boolean getBoolean(String column)
    {
        Object obj = getObject(column);
        if (obj instanceof Boolean)
        {
            return (Boolean) obj;
        }

        return null;
    }

    public List<Row> getChildRows(int index)
    {
        if (childRows == null)
        {
            childRows = new ChildRows[tableAlias.getChildAliases().size()];
        }

        List<Row> rows = childRows[index];
        if (rows == null)
        {
            rows = new ChildRows();
            childRows[index] = rows;
        }

        return rows;
    }

    public int getPos()
    {
        return pos;
    }

    public int getSubPos()
    {
        return subPos;
    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    /** Get single parent. Either returns temporary predicate parent or first connected parent */
    public Row getParent()
    {
        if (predicateParent != null)
        {
            return predicateParent;
        }
        else if (parents != null && parents.size() > 0)
        {
            return parents.get(0);
        }

        return null;
    }

    /** Add provided row as parent to this row */
    public void addParent(Row row)
    {
        if (parents == null)
        {
            parents = new ArrayList<>();
        }
        parents.add(row);
    }

    public List<Row> getParents()
    {
        return defaultIfNull(parents, emptyList());
    }

    public void setPredicateParent(Row parent)
    {
        predicateParent = parent;
    }

    public void clearPredicateParent()
    {
        predicateParent = null;
    }

    /** Construct a row with provided meta values and position */
    public static Row of(TableAlias table, int pos, Object... values)
    {
        Row t = new Row();
        t.pos = pos;
        t.tableAlias = table;
        t.values = new ObjectValues(values);
        return t;
    }

    /** Construct a row with provided meta values and position */
    public static Row of(TableAlias table, int pos, Values values)
    {
        Row t = new Row();
        t.pos = pos;
        t.tableAlias = table;
        t.values = values;
        return t;
    }

    @Override
    public int hashCode()
    {
        return 17 + (pos * 37) + (subPos * 37);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Row)
        {
            // Assumes the same table
            return ((Row) obj).pos == pos && ((Row) obj).subPos == subPos;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return tableAlias.getTable() + " (" + pos + ") " + values;
    }

    /** Child rows list. For easier instanceof etc. to detect if a value is child rows */
    public static class ChildRows extends ArrayList<Row>
    {
    }

    /** Values definition of a rows values */
    public interface Values
    {
        Object get(int ordinal);
    }

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
