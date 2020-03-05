package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Populating join type. Does not produce
 * tuples but instead populates outer rows with join rows */
public class PopulatingJoin extends JoinItem
{
    private final List<SortItem> orderBy;
    private final List<Expression> groupBy;
    private final List<JoinItem> joins;
    private final Expression having;
    
    public PopulatingJoin(
            List<SortItem> orderBy,
            List<Expression> groupBy,
            List<JoinItem> joins,
            Expression having)
    {
        this.orderBy = requireNonNull(orderBy, "orderBy");
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.joins = requireNonNull(joins, "joins");
        this.having = having;
    }
    
    public List<JoinItem> getJoins()
    {
        return joins;
    }
    
    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }
    
    public List<Expression> getGroupBy()
    {
        return groupBy;
    }
    
    public Expression getHaving()
    {
        return having;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append(System.lineSeparator());
        joins.forEach(j -> sb.append("\t").append(j.toString()).append(System.lineSeparator()));
        sb.append("}").append(System.lineSeparator());
        return sb.toString();
    }
}
