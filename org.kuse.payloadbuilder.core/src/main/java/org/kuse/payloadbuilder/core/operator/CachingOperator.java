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
import static org.apache.commons.lang3.StringUtils.repeat;

import java.util.ArrayList;
import java.util.List;

import org.kuse.payloadbuilder.core.operator.OperatorContext.NodeData;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Caches provided operator to allow rewinds (Used in inner operator for nested loop) */
class CachingOperator extends AOperator
{
    private final Operator operator;

    /* Statistics */
    private int executionCount;

    CachingOperator(int nodeId, Operator target)
    {
        super(nodeId);
        this.operator = requireNonNull(target, "operator");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }

    @Override
    public String getName()
    {
        return "Cache";
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        executionCount++;

        Data data = context.getOperatorContext().getNodeData(nodeId, Data::new);

        if (data.cache == null)
        {
            data.cache = new ArrayList<>();
            RowIterator it = operator.open(context);
            while (it.hasNext())
            {
                data.cache.add(it.next());
            }
            it.close();
        }
        return RowIterator.wrap(data.cache.iterator());
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof CachingOperator)
        {
            CachingOperator that = (CachingOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator);
        }
        return super.equals(obj);
    }

    @Override
    public String toString(int indent)
    {
        String indentString = repeat("  ", indent);
        return String.format("CACHING (ID: %d, EXECUTION COUNT: %d)", nodeId, executionCount) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Context data for this operator */
    static class Data extends NodeData
    {
        List<Row> cache;
    }
}
