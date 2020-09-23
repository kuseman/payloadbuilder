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
import java.util.Map;

import org.apache.commons.collections.iterators.TransformIterator;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Grouped row. Result of a {@link GroupByOperator} */
class GroupedRow implements Tuple
{
    private final List<Tuple> tuples;
    /** Set of columns in group expressions, these columns should not return an aggregated value */
    private final Map<String, QualifiedName> columnReferences;

    GroupedRow(List<Tuple> tuples, Map<String, QualifiedName> columnReferences)
    {
        if (isEmpty(tuples))
        {
            throw new RuntimeException("Rows cannot be empty.");
        }
        this.tuples = tuples;
        this.columnReferences = columnReferences;
    }

    @Override
    public boolean containsAlias(String alias)
    {
        return tuples.get(0).containsAlias(alias);
    }

    @Override
    public Object getValue(QualifiedName qname, int partIndex)
    {
        String column = qname.getLast();
        QualifiedName columnRef = columnReferences.get(column);
        if (columnRef != null)
        {
            return tuples.get(0).getValue(columnRef, 0);
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
}
