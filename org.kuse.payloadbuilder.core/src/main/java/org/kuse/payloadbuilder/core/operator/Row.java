package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

/** Row */
public class Row
{
    protected int pos;
    // If this is a tuple row then subPos is the other rows position
    private int subPos;
    protected TableAlias tableAlias;
    private String[] columns;

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

    Row(Row source, int subPos)
    {
        this.pos = source.pos;
        this.subPos = subPos;
        this.tableAlias = source.tableAlias;
        this.values = source.values;
        this.childRows = copyChildRows(source);
        this.parents = source.parents;
        this.columns = source.columns;
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

    /** Return columns for this row */
    public String[] getColumns()
    {
        return columns != null ? columns : tableAlias.getColumns();
    }
    
    public int getColumnCount()
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
        int ordinal = ArrayUtils.indexOf(getColumns(), column);
        return getObject(ordinal);
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
    void addParent(Row row)
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

    void setPredicateParent(Row parent)
    {
        predicateParent = parent;
    }

    void clearPredicateParent()
    {
        predicateParent = null;
    }

    /** Construct a row with provided alias, values and position */
    public static Row of(TableAlias alias, int pos, Object... values)
    {
        return of(alias , pos, alias.getColumns(), values);
    }

    /** Construct a row with provided parent, alias, values and position */
    public static Row of(Row parent, TableAlias alias, int pos, Object... values)
    {
        return of(parent, alias, pos, alias.getColumns(), values);
    }

    /** Construct a row with provided alias, columns, values and position */
    public static Row of(TableAlias alias, int pos, String[] columns, Object... values)
    {
        return of(alias, pos, columns, new ObjectValues(values));
    }

    /** Construct a row with provided parent, alias, columns, values and position */
    public static Row of(Row parent, TableAlias alias, int pos, String[] columns, Object... values)
    {
        return of(parent, alias, pos, columns, new ObjectValues(values));
    }

    /** Construct a row with provided alias, values and position */
    public static Row of(TableAlias alias, int pos, Values values)
    {
        return of(alias, pos, alias.getColumns(), values);
    }
    
    /** Construct a row with provided alias, columns, values and position */
    public static Row of(TableAlias alias, int pos, String[] columns, Values values)
    {
        return of(null, alias,pos, columns, values);
    }
    
    /** Construct a row with provided parent, alias, columns, values and position */
    private static Row of(Row parent, TableAlias alias, int pos, String[] columns, Values values)
    {
        Row t = new Row();
        t.pos = pos;
        t.tableAlias = alias;
        t.columns = columns;
        t.values = values;
        if (parent != null)
        {
            t.addParent(parent);
        }
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
