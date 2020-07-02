package com.viskan.payloadbuilder.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

/** Select */
public class Select extends ASelectNode
{
    private final List<SelectItem> selectItems;
    private final TableSourceJoined from;
    private final Expression where;
    private final List<Expression> groupBy;
    private final List<SortItem> orderBy;
    
    Select(List<SelectItem> selectItems,
            TableSourceJoined from,
            Expression where,
            List<Expression> groupBy,
            List<SortItem> orderBy)
    {
        this.selectItems = requireNonNull(selectItems, "selectItems");
        this.from = from;
        this.where = where;
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.orderBy = requireNonNull(orderBy, "orderBy");
    }
    
    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }
    
    public TableSourceJoined getFrom()
    {
        return from;
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
    
    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();  
        sb.append("SELECT");
        sb.append(System.lineSeparator());
        sb.append(selectItems.stream().map(s -> s.toString()).collect(joining("," + System.lineSeparator(), "", System.lineSeparator())));
        sb.append("FROM ").append(from);
        if (where != null)
        {
            sb.append(System.lineSeparator());
            sb.append("WHERE ").append(where);
        }
        
        if (!orderBy.isEmpty())
        {
            sb.append(System.lineSeparator());
            sb.append("ORDER BY ");
            sb.append(orderBy.stream().map(o -> o.toString()).collect(joining(", ")));
        }
        return sb.toString();
    }
}
