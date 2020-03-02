package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

/** Projection that projects a column path  */
class ColumnPathProjection implements Projection
{
    final String[] parts;

    /** Number of parent steps from current alias */
    private int parentSteps;

    /** Index path from current or parent */
    private int[] aliasIndexPath;

    /** Ordinal of column if any */
    private int ordinal = -2;
    private boolean empty;

    ColumnPathProjection(String path)
    {
        this.parts = requireNonNull(path).split("\\.");
    }
    
    /** Returns value from this projection */
    Object getValue(Row row)
    {
        if (ordinal == -2)
        {
            calculate(row.getTableAlias());
        }
        
        return getValueFromPath(row);
    }

    @Override
    public void writeValue(OutputWriter writer, Row row)
    {        
        writer.writeValue(getValue(row));
    }

    private Object getValueFromPath(Row row)
    {
        if (empty)
        {
            return null;
        }

        Row current = row;
        List<Row> rows = null;
        for (int index : aliasIndexPath)
        {
            rows = current.getChildRows(index);
            if (CollectionUtils.isEmpty(rows))
            {
                return null;
            }

            current = rows.get(0);
        }

        return ordinal >= 0 ? current.getObject(ordinal) : rows;
    }

    private void calculate(TableAlias tableAlias)
    {
        TIntList path = new TIntArrayList();
        TableAlias current = tableAlias;
        for (int i = 0; i < parts.length; i++)
        {
            String part = parts[i];

            // Alias pointing to current, move to next
            if (Objects.equals(current.getAlias(), part))
            {
                continue;
            }

            // Prio 1: child alias
            TableAlias childAlias = current.getChildAlias(part);
            if (childAlias != null)
            {
                path.add(childAlias.getParentIndex());
                current = childAlias;
                continue;
            }
            else if (i == parts.length - 1)
            {
                // Column ordinal
                ordinal = ArrayUtils.indexOf(current.getColumns(), part);
            }
            else
            {
                // No child alias found
                break;
            }
        }
        aliasIndexPath = path.toArray();
        ordinal = Math.max(ordinal, -1);
        
        // No path found
        if (ordinal <= -1 && aliasIndexPath.length == 0)
        {
            empty = true;
        }
    }
}
