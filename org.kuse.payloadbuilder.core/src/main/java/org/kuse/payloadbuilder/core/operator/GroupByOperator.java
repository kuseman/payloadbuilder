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

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Operator that groups by a bucket function */
class GroupByOperator extends AOperator
{
    private final Operator operator;
    private final ValuesExtractor valuesExtractor;
    private final int size;
    private final Set<QualifiedName> columnReferences;

    GroupByOperator(
            int nodeId,
            Operator operator,
            Set<QualifiedName> columnReferences,
            ValuesExtractor valuesExtractor,
            int size)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.columnReferences = requireNonNull(columnReferences, "columnReferences");
        this.valuesExtractor = requireNonNull(valuesExtractor, "valuesExtractor");
        this.size = size;
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }

    @Override
    public String getName()
    {
        return "Group by";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry("Values", valuesExtractor.toString()));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        ArrayKey key = new ArrayKey();
        key.key = new Object[size];

        Map<ArrayKey, List<Tuple>> table = new LinkedHashMap<>();
        RowIterator it = operator.open(context);
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            valuesExtractor.extract(context, tuple, key.key);
            table.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
        }
        it.close();

        Iterator<List<Tuple>> iterator = table.values().iterator();
        return new RowIterator()
        {
            @Override
            public Tuple next()
            {
                return new GroupedRow(iterator.next(), columnReferences);
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return 17
            + (37 * operator.hashCode())
            + (37 * valuesExtractor.hashCode())
            + (37 * size);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof GroupByOperator)
        {
            GroupByOperator that = (GroupByOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator)
                && columnReferences.equals(that.columnReferences)
                && valuesExtractor.equals(that.valuesExtractor)
                && size == that.size;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("GROUP BY (ID: %d, VALUES: %s)", nodeId, valuesExtractor) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /**
     * <pre>
     * NOTE! Special key class used in hash map.
     * To avoid allocations and use the same object array for each row a single instance is used of this class.
     * It's safe because the values are iterated after grouping and hence we don't need to find the values again by key.
     * </pre>
     **/
    private static class ArrayKey
    {
        private Object[] key;

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(key);
        }

        @Override
        public boolean equals(Object obj)
        {
            return true;
        }
    }
}
