package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class Query
{
    private final List<SelectItem> selectItems;
    private final QualifiedName from;
    private final String alias;
    private final List<Join> relations;
    private final Expression where;
    private final List<Expression> groupBy;
    private final List<SortItem> orderBy;
    
    public Query(List<SelectItem> selectItems,
            QualifiedName from,
            String alias,
            List<Join> relations,
            Expression where,
            List<Expression> groupBy,
            List<SortItem> orderBy)
    {
        this.selectItems = requireNonNull(selectItems, "selectItems");
        this.from = requireNonNull(from, "from");
        this.alias = requireNonNull(alias, "alias");
        this.relations = requireNonNull(relations, "from");
        this.where = where;
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.orderBy = requireNonNull(orderBy, "orderBy");
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();  
        sb.append("SELECT");
        sb.append(System.lineSeparator());
        sb.append(selectItems.stream().map(s -> s.toString()).collect(joining("," + System.lineSeparator(), "", System.lineSeparator())));
        sb.append("FROM ").append(from).append(" ").append(alias);
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
