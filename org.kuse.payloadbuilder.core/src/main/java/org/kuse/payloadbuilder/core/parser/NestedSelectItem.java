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
import static java.util.stream.Collectors.joining;

import java.util.List;

public class NestedSelectItem extends SelectItem
{
    private final List<SelectItem> selectItems;
    private final Expression from;
    private final Expression where;
    private final List<Expression> groupBy;
    private final List<SortItem> orderBy;
    private final Type type;

    public NestedSelectItem(
            Type type,
            List<SelectItem> selectItems,
            Expression from,
            Expression where,
            String identifier,
            List<Expression> groupBy,
            List<SortItem> orderBy)
    {
        super(identifier, identifier != null);
        this.type = requireNonNull(type, "type");
        this.selectItems = requireNonNull(selectItems, "selectItems");
        this.from = from;
        this.where = where;
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.orderBy = requireNonNull(orderBy, "orderBy");
    }

    public Expression getFrom()
    {
        return from;
    }

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }

    public Expression getWhere()
    {
        return where;
    }

    public List<Expression> getGroupBy()
    {
        return groupBy;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public Type getType()
    {
        return type;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append(" (");
        sb.append(System.lineSeparator());
        sb.append(selectItems.stream().map(s -> s.toString()).collect(joining("," + System.lineSeparator(), "", System.lineSeparator())));
        if (from != null)
        {
            sb.append("FROM ").append(from);
        }
        if (where != null)
        {
            sb.append(System.lineSeparator());
            sb.append("WHERE ").append(where);
        }
        sb.append(")");
        sb.append(super.toString());
        return sb.toString();
    }

    public enum Type
    {
        OBJECT,
        ARRAY;
    }
}
