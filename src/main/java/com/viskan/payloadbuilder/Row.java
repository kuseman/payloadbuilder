package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

public class Row
{
    protected int pos;
    // If this is a tuple row then subPos is the other rows position
    private int subPos;
    protected TableAlias tableAlias;
    
    /** Collection of parents that this row belongs to */
    private List<Row> parents;
    
    /** Temporary collection of parents during predicate evaluation */
    private final List<Row> predicateParent = new ArrayList<>(1);
    
    private Object[] values;
    private List<List<Row>> childRows;

    Row()
    {}
    
    public Row(Row source, int subPos)
    {
        this.pos = source.pos;
        this.subPos = subPos;
        this.tableAlias = source.tableAlias;
        this.values = source.values;
        this.childRows = copyChildRows(source);
        this.parents = source.parents;
    }

    private List<List<Row>> copyChildRows(Row source)
    {
        if (source.childRows == null)
        {
            return null;
        }
        
        List<List<Row>> copy = new ArrayList<>(source.childRows.size());
        for (List<Row> rows : source.childRows)
        {
            copy.add(new ArrayList<>(rows));
        }
        
        return copy;
    }
    
    public Object getObject(int ordinal)
    {
        if (ordinal < 0 || ordinal >= values.length)
        {
            return null;
        }
        return values[ordinal];
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
            childRows = new ArrayList<>(5);
        }

        List<Row> rows = null;
        if (childRows.size() < index + 1)
        {
            rows = new ArrayList<>();
            childRows.add(rows);
        }
        else
        {
            rows = childRows.get(index);
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
    
    public List<Row> getParents()
    {
        if (!predicateParent.isEmpty())
        {
            return predicateParent;
        }
        return getRealParents();
    }
    
    private List<Row> getRealParents()
    {
        if (parents == null)
        {
            parents = new ArrayList<>();
        }
        return parents;
    }
    
//    /**
//     * Merge provided inner row
//     * @param inner Row to merge
//     * @param limit Limit the count of inner rows to merge
//     **/
//    public void merge(Row inner, int limit)
//    {
//        if (childRows == null)
//        {
//            childRows = new ArrayList<>(5);
//        }
//
//        List<Row> rows = null;
//        if (childRows.size() < inner.tableAlias.parentIndex + 1)
//        {
//            rows = new ArrayList<>();
//            childRows.add(rows);
//        }
//        else
//        {
//            rows = childRows.get(inner.tableAlias.parentIndex);
//        }
//
//        if (limit < 0 || rows.size() < limit)
//        {
//            inner.getRealParents().add(this);
//            rows.add(inner);
//        }
//    }
    
    /** Evaluate this row with provided parent and predicate
     * Temporary sets provided parent as parent row before evaluating
     * and then reverts.
     * This is because the parent might not be a real parent (only known
     * after evaluation). 
     **/
    public boolean evaluatePredicate(Row parent, EvaluationContext context, BiPredicate<EvaluationContext, Row> predicate)
    {
        predicateParent.add(parent);
        boolean result = predicate.test(context, this);
        predicateParent.clear();
        return result;
    }

    /** Construct a row with provided meta values and position */
    public static Row of(TableAlias table, int pos, Object... values)
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
        return tableAlias.getTable() + " (" + pos + ") " + Arrays.toString(values) + " " + (!CollectionUtils.isEmpty(childRows) ? (System.lineSeparator() + childRows.stream().map(r -> r.toString() + System.lineSeparator()).collect(joining(","))) : "");
    }
}
