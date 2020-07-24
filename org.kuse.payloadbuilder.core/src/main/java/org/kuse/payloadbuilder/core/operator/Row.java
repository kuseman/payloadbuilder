package org.kuse.payloadbuilder.core.operator;

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

    @SuppressWarnings("unchecked")
    public List<Row> getChildRows(int index)
    {
        if (childRows == null)
        {
            childRows = new List[tableAlias.getChildAliases().size()];
        }

        List<Row> rows = childRows[index];
        if (rows == null)
        {
            rows = new ArrayList<>();
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

    public List<Row> getParents()
    {
        if (parents == null)
        {
            parents = new ArrayList<>();
        }
        return parents;
    }

    public void setPredicateParent(Row parent)
    {
        predicateParent = parent;
    }

    public Row getPredicateParent()
    {
        return predicateParent;
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
