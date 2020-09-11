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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Operator sorting target operator */
class SortByOperator extends AOperator
{
    private final Operator target;
    private final RowComparator comparator;

    SortByOperator(int nodeId, Operator target, RowComparator comparator)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.comparator = requireNonNull(comparator, "comparator");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(target);
    }

    @Override
    public String getName()
    {
        return "Order by";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry("Values", comparator.toString()));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        List<Row> rows = new ArrayList<>();
        RowIterator it = target.open(context);
        while (it.hasNext())
        {
            rows.add(it.next());
        }
        it.close();
        Collections.sort(rows, (rowA, rowB) -> comparator.compare(context, rowA, rowB));
        return RowIterator.wrap(rows.iterator());
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SortByOperator)
        {
            SortByOperator that = (SortByOperator) obj;
            return target.equals(that.target)
                && comparator.equals(that.comparator);
        }

        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("ORDER BY (ID: %d, VALUES: %s)", nodeId, comparator) + System.lineSeparator()
            +
            indentString + target.toString(indent + 1);
    }
}
