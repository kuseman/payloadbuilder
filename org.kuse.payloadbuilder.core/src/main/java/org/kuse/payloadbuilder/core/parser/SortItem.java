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
package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

public class SortItem extends ASelectNode
{
    private final Expression expression;
    private final Order order;
    private final NullOrder nullOrder;

    public SortItem(Expression expression, Order order, NullOrder nullOrder)
    {
        this.expression = requireNonNull(expression, "expression");
        this.order = requireNonNull(order, "order");
        this.nullOrder = requireNonNull(nullOrder, "nullOrder");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Order getOrder()
    {
        return order;
    }

    public NullOrder getNullOrder()
    {
        return nullOrder;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return 17
            + (37 * expression.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SortItem)
        {
            SortItem that = (SortItem) obj;
            return expression.equals(that.expression)
                && order == that.order
                && nullOrder == that.nullOrder;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return expression + " " + order + (nullOrder != NullOrder.UNDEFINED ? (" NULLS " + nullOrder) : "");
    }

    public enum NullOrder
    {
        FIRST, LAST, UNDEFINED;
    }

    public enum Order
    {
        ASC, DESC;
    }
}
