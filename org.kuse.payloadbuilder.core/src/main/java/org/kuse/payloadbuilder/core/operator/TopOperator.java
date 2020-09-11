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

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Operator for TOP expressions */
class TopOperator extends AOperator
{
    private final Operator target;
    private final Expression topExpression;

    TopOperator(int nodeId, Operator target, Expression topExpression)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.topExpression = requireNonNull(topExpression, "topExpression");
    }

    @Override
    public String getName()
    {
        return "TOP";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return MapUtils.ofEntries(MapUtils.entry("Value", topExpression));
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return singletonList(target);
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        Object obj = topExpression.eval(context);
        if (!(obj instanceof Integer) || (Integer) obj < 0)
        {
            throw new OperatorException("Top expression " + topExpression + " should return a zero or positive Integer. Got: " + obj);
        }
        final int top = ((Integer) obj).intValue();
        final RowIterator it = target.open(context);
        return new RowIterator()
        {
            private int count;

            @Override
            public Row next()
            {
                count++;
                return it.next();
            }

            @Override
            public boolean hasNext()
            {
                return count < top && it.hasNext();
            }

            @Override
            public void close()
            {
                it.close();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TopOperator)
        {
            TopOperator that = (TopOperator) obj;
            return target.equals(that.target)
                && topExpression.equals(that.topExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return super.toString();
    }
}
