package com.viskan.payloadbuilder.parser.tree;

/** Visitor adapter for tree visitors */
public abstract class ATreeVisitor<TR, TC> implements TreeVisitor<TR, TC>
{
    /**
     * Visit expression.
     * Override this method to visit any expression found in tree components
     * @param expression Expression to visit
     * @param context Context for visitor
     *  */
    protected void visit(Expression expression, TC context)
    {
    }
    
    @Override
    public TR visit(Query query, TC context)
    {
        query.getFrom().accept(this, context);
        if (query.getWhere() != null)
        {
            visit(query.getWhere(), context);
        }
        query.getOrderBy().forEach(o -> o.accept(this, context));
        query.getGroupBy().forEach(e -> visit(e, context));
        query.getSelectItems().forEach(s -> s.accept(this, context));
        return null;
    }

    @Override
    public TR visit(JoinedTableSource joinedTableSource, TC context)
    {
        joinedTableSource.getTableSource().accept(this, context);
        joinedTableSource.getJoins().forEach(j -> j.accept(this, context));
        return null;
    }

    @Override
    public TR visit(SortItem sortItem, TC context)
    {
        visit(sortItem.getExpression(), context);
        return null;
    }

    @Override
    public TR visit(ExpressionSelectItem expressionSelectItem, TC context)
    {
        visit(expressionSelectItem.getExpression(), context);
        return null;
    }

    @Override
    public TR visit(NestedSelectItem nestedSelectItem, TC context)
    {
        if (nestedSelectItem.getFrom() != null)
        {
            visit(nestedSelectItem.getFrom(), context);
        }
        nestedSelectItem.getSelectItems().forEach(s -> s.accept(this, context));
        if (nestedSelectItem.getWhere() != null)
        {
            visit(nestedSelectItem.getWhere(), context);
        }
        return null;
    }

    @Override
    public TR visit(Table table, TC context)
    {
        return null;
    }

    @Override
    public TR visit(TableFunction tableFunction, TC context)
    {
        tableFunction.getArguments().forEach(a -> visit(a, context));
        return null;
    }

    @Override
    public TR visit(Join join, TC context)
    {
        join.getJoinedTableSource().accept(this, context);
        visit(join.getCondition(), context);
        return null;
    }

    @Override
    public TR visit(Apply apply, TC context)
    {
        apply.getJoinedTableSource().accept(this, context);
        return null;
    }

    @Override
    public TR visit(PopulatingJoin populatingJoin, TC context)
    {
        populatingJoin.getJoins().forEach(j -> j.accept(this, context));
        populatingJoin.getGroupBy().forEach(e -> visit(e, context));
        if (populatingJoin.getWhere() != null)
        {
            visit(populatingJoin.getWhere(), context);
        }
        populatingJoin.getOrderBy().forEach(o -> o.accept(this, context));
        return null;
    }
}
