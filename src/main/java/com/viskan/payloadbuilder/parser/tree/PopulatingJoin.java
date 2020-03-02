package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Populating join type. Does not produce
 * tuples but instead populates outer rows with join rows */
public class PopulatingJoin extends Join
{
    private final List<SortItem> orderBy;
    private final List<Expression> groupBy;
    private final Expression having;
    private final List<Join> subJoins;
    
    public PopulatingJoin(
            QualifiedName qname,
            String alias,
            JoinType type,
            Expression condition,
            List<Join> subJoins,
            List<SortItem> orderBy,
            List<Expression> groupBy,
            Expression having)
    {
        super(qname, alias, type, condition);
        this.orderBy = requireNonNull(orderBy, "orderBy");
        this.groupBy = requireNonNull(groupBy, "groupBy");
        this.having = requireNonNull(having, "having");
        this.subJoins = requireNonNull(subJoins, "subJoins");
    }
}
