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

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.lang3.ArrayUtils;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/** Grouped row */
class GroupedRow extends Row
{
    private final List<Row> rows;
    /** Set of column ordinals in group expressions, these ordinals should not return an aggregated value */
    private final TIntSet columnOrdinals;

    GroupedRow(List<Row> rows, int pos, List<String> columnReferences)
    {
        if (isEmpty(rows))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.rows = rows;
        this.pos = pos;
        this.columnOrdinals = getColumnOrdinals(columnReferences);
        super.tableAlias = rows.get(0).getTableAlias();
    }

    private TIntSet getColumnOrdinals(List<String> columnReferences)
    {
        Row row = rows.get(0);
        TIntSet result = new TIntHashSet(columnReferences.size());
        for (String column : columnReferences)
        {
            int index = ArrayUtils.indexOf(row.getColumns(), column);
            result.add(index);
        }
        return result;
    }

    @Override
    public Object getObject(int ordinal)
    {
        if (columnOrdinals.contains(ordinal))
        {
            return rows.get(0).getObject(ordinal);
        }
        return new TransformIterator(rows.iterator(), row -> ((Row) row).getObject(ordinal));
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Row> getChildRows(int index)
    {
        if (childRows == null)
        {
            childRows = new List[tableAlias.getChildAliases().size()];
        }

        ArrayList<Row> rows = (ArrayList<Row>) childRows[index];
        if (rows == null)
        {
            for (Row groupRow : this.rows)
            {
                List<Row> childRows = groupRow.getChildRows(index);
                if (rows == null)
                {
                    rows = new ArrayList<>(childRows);
                }
                else
                {
                    rows.ensureCapacity(rows.size() + childRows.size());
                    rows.addAll(childRows);
                }
            }

            childRows[index] = rows;
        }

        return rows;
    }

    @Override
    public int hashCode()
    {
        return 17 + (pos * 37);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof GroupedRow)
        {
            // Assume grouped row
            return ((GroupedRow) obj).pos == pos;
        }
        return false;
    }
}
