/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
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
    protected int pos;
    // If this is a tuple row then subPos is the other rows position
    //    private int subPos;
    protected TableAlias tableAlias;
    protected String[] columns;

    //    /** Collection of parents that this row belongs to */
    //    private List<Row> parents;
    //
    //    /** Temporary parent that is set during predicate evaluations (ie. join conditions) */
    //    private Row predicateParent;

    private Values values;
    //    protected List<ChildRows> childRowsCollection;

    /** Temporary fields used by physical operators during join */
    //    boolean match;
    //    int hash;
    //    Object[] extractedValues;

    Row()
    {
    }

    //    Row(Row source, int subPos)
    //    {
    //        this.pos = source.pos;
    //        this.subPos = subPos;
    //        this.tableAlias = source.tableAlias;
    //        this.values = source.values;
    ////        this.childRowsCollection = copyChildRows(source);
    ////        this.parents = source.parents;
    //        this.columns = source.columns;
    //    }

    //    private List<ChildRows> copyChildRows(Row source)
    //    {
    //        if (source.childRowsCollection == null)
    //        {
    //            return null;
    //        }
    //
    //        int size = source.childRowsCollection.size();
    //        List<ChildRows> copy = new ArrayList<>(size);
    //        for (int i = 0; i < size; i++)
    //        {
    //            copy.add(new ChildRows(source.childRowsCollection.get(i)));
    //        }
    //
    //        return copy;
    //    }

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
        //        for (int i=partIndex;i<size;i++)
        //        {
        //
        //        }

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

    //    @Override
    //    public void writeColumns(OutputWriter writer, String alias)
    //    {
    //        String[] columns = getColumns();
    //        int length = columns.length;
    //        for (int i = 0; i < length; i++)
    //        {
    //            writer.writeFieldName(columns[i]);
    //            writer.writeValue(values.get(i));
    //        }
    //    }

    public Object getObject(int ordinal)
    {
        if (ordinal < 0)
        {
            return null;
        }
        return values.get(ordinal);
    }

    private static final String POS = "__pos";

    public Object getObject(String column)
    {
        if (POS.equals(column))
        {
            return pos;
        }
        int ordinal = ArrayUtils.indexOf(getColumns(), column);
        return getObject(ordinal);
    }

    //    public List<Row> getChildRows(TableAlias alias)
    //    {
    //        if (childRowsCollection == null)
    //        {
    //            childRowsCollection = new ArrayList<>();
    //        }
    //
    //        int size = childRowsCollection.size();
    //        for (int i = 0; i < size; i++)
    //        {
    //            ChildRows childRows = childRowsCollection.get(i);
    //            if (childRows.alias == alias)
    //            {
    //                return childRows;
    //            }
    //        }
    //
    //        ChildRows childRows = new ChildRows(alias);
    //        childRowsCollection.add(childRows);
    //
    //        return childRows;
    //    }
    //
    //    public int getPos()
    //    {
    //        return pos;
    //    }
    //
    //    public int getSubPos()
    //    {
    //        return subPos;
    //    }

    public TableAlias getTableAlias()
    {
        return tableAlias;
    }

    /** Get single parent. Either returns temporary predicate parent or first connected parent */
    //    public Row getParent()
    //    {
    //        if (predicateParent != null)
    //        {
    //            return predicateParent;
    //        }
    //        else if (parents != null && parents.size() > 0)
    //        {
    //            return parents.get(0);
    //        }
    //
    //        return null;
    //    }
    //
    //    /** Add provided row as parent to this row */
    //    void addParent(Row row)
    //    {
    //        if (parents == null)
    //        {
    //            parents = new ArrayList<>();
    //        }
    //        parents.add(row);
    //    }
    //
    //    public List<Row> getParents()
    //    {
    //        return defaultIfNull(parents, emptyList());
    //    }
    //
    //    void setPredicateParent(Row parent)
    //    {
    //        predicateParent = parent;
    //    }
    //
    //    void clearPredicateParent()
    //    {
    //        predicateParent = null;
    //    }

    public static Row of(TableAlias alias, int pos, Object... values)
    /** Construct a row with provided alias, values and position */
    {
        return of(alias, pos, alias.getColumns(), values);
    }

    /** Construct a row with provided alias, columns, values and position */
    public static Row of(TableAlias alias, int pos, String[] columns, Object... values)
    {
        return of(alias, pos, columns, new ObjectValues(values));
    }

    /** Construct a row with provided alias, values and position */
    public static Row of(TableAlias alias, int pos, Values values)
    {
        return of(alias, pos, alias.getColumns(), values);
    }

    /** Construct a row with provided parent, alias, columns, values and position */
    private static Row of(TableAlias alias, int pos, String[] columns, Values values)
    {
        Row t = new Row();
        t.pos = pos;
        t.tableAlias = alias;
        t.columns = columns;
        t.values = values;
        return t;
    }

    @Override
    public int hashCode()
    {
        return 17 + (pos * 37);
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

    //    /** Child rows list. List of rows with a specific table alias */
    //    public static class ChildRows extends ArrayList<Row>
    //    {
    //        private final TableAlias alias;
    //        /** Flag used by {@link GroupedRow} to indicate if this collection is populted or not */
    //        boolean populated;
    //
    //        ChildRows(TableAlias alias)
    //        {
    //            this.alias = alias;
    //        }
    //
    //        ChildRows(ChildRows childRows)
    //        {
    //            this.alias = childRows.alias;
    //            addAll(childRows);
    //        }
    //
    //    }

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
