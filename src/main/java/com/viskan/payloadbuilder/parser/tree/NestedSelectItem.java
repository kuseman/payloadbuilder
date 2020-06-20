package com.viskan.payloadbuilder.parser.tree;

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
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
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
