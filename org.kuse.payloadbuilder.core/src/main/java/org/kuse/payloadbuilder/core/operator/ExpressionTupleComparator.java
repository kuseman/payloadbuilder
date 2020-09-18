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

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ExpressionMath;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Interpreter based row comparator */
class ExpressionTupleComparator implements TupleComparator
{
    private final List<SortItem> items;

    ExpressionTupleComparator(List<SortItem> items)
    {
        this.items = requireNonNull(items);
    }

    @Override
    public int compare(ExecutionContext context, Tuple a, Tuple b)
    {
        int size = items.size();

        for (int i = 0; i < size; i++)
        {
            SortItem item = items.get(i);
            NullOrder nullOrder = item.getNullOrder();
            Order order = item.getOrder();
            Expression e = item.getExpression();

            context.setTuple(a);
            Object aValue = e.eval(context);
            context.setTuple(b);
            Object bValue = e.eval(context);

            if (aValue == null && bValue == null)
            {
                continue;
            }
            else if (aValue == null)
            {
                // Null is always less if not specified
                return nullOrder == NullOrder.FIRST
                    || nullOrder == NullOrder.UNDEFINED ? -1 : 1;
            }
            else if (bValue == null)
            {
                // Null is always less if not specified
                return nullOrder == NullOrder.FIRST
                    || nullOrder == NullOrder.UNDEFINED ? 1 : -1;
            }

            int c = ExpressionMath.cmp(aValue, bValue);
            if (c != 0)
            {
                return c * (order == Order.DESC ? -1 : 1);
            }
        }

        return 0;
    }

    @Override
    public int hashCode()
    {
        return items.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionTupleComparator)
        {
            ExpressionTupleComparator that = (ExpressionTupleComparator) obj;
            return items.equals(that.items);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return items.toString();
    }
}
