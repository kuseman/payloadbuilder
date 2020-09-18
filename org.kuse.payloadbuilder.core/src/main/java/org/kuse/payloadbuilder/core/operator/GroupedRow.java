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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.iterators.TransformIterator;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Grouped row. Result of a {@link GroupByOperator} */
class GroupedRow implements Tuple
{
    private final List<Tuple> tuples;
    /** Set of columns in group expressions, these columns should not return an aggregated value */
    private final Set<QualifiedName> columnReferences;

    GroupedRow(List<Tuple> tuples, Set<QualifiedName> columnReferences)
    {
        if (isEmpty(tuples))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.tuples = tuples;
        //        this.pos = pos;
        this.columnReferences = columnReferences;
        // TODO: Combine columns from all group rows since we might have other columns further "down"
        //        this.columns = tuples.get(0).columns;
        //        super.tableAlias = tuples.get(0).getTableAlias();
    }

    //    private TIntSet getColumnOrdinals(List<String> columnReferences)
    //    {
    //        Tuple tuple = tuples.get(0);
    //        TIntSet result = new TIntHashSet(columnReferences.size());
    ////        for (String column : columnReferences)
    ////        {
    ////            int index = ArrayUtils.indexOf(row.getColumns(), column);
    ////            result.add(index);
    ////        }
    //        return result;
    //    }

    @Override
    public boolean containsAlias(String alias)
    {
        return tuples.get(0).containsAlias(alias);
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        if (columnReferences.contains(qname))
        {
            return tuples.get(0).getValue(qname, partIndex);
        }

        return new TransformIterator(tuples.iterator(), tuple -> ((Tuple) tuple).getValue(qname, partIndex));
    }

    @Override
    public Iterator<QualifiedName> getQualifiedNames()
    {
        // Use first row here.
        // TODO: Might need to distinct all grouped rows qnames since there might be different ones further down
        return tuples.get(0).getQualifiedNames();
    }

    //    @Override
    //    public void writeColumns(OutputWriter writer, String alias)
    //    {
    //        
    //    }

    //    @Override
    //    public Object getObject(int ordinal)
    //    {
    //        if (columnOrdinals.contains(ordinal))
    //        {
    //            return rows.get(0).getObject(ordinal);
    //        }
    //        return new TransformIterator(rows.iterator(), row -> ((Row) row).getObject(ordinal));
    //    }
    //
    //    @Override
    //    public List<Row> getChildRows(TableAlias alias)
    //    {
    //        ChildRows childRows = (ChildRows) super.getChildRows(alias);
    //
    //        if (!childRows.populated)
    //        {
    //            for (Row groupRow : this.rows)
    //            {
    //                List<Row> rows = groupRow.getChildRows(alias);
    //                childRows.ensureCapacity(rows.size() + childRows.size());
    //                childRows.addAll(childRows);
    //            }
    //            childRows.populated = true;
    //        }
    //
    //        return childRows;
    //    }

    //    @Override
    //    public int hashCode()
    //    {
    //        return 17 + (pos * 37);
    //    }
    //
    //    @Override
    //    public boolean equals(Object obj)
    //    {
    //        if (obj instanceof GroupedRow)
    //        {
    //            // Assume grouped row
    //            return ((GroupedRow) obj).pos == pos;
    //        }
    //        return false;
    //    }
}
