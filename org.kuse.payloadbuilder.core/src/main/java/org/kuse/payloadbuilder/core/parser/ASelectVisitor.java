package org.kuse.payloadbuilder.core.parser;

/** Visitor adapter for select visitors
 * @param <TR> Result type
 * @param <TC> Context type
 **/
public abstract class ASelectVisitor<TR, TC> implements SelectVisitor<TR, TC>
{
    /**
     * Visit expression. Override this method to visit any expression found in tree components
     *
     * @param expression Expression to visit
     * @param context Context for visitor
     */
    protected void visit(Expression expression, TC context)
    {
    }

    @Override
    public TR visit(Select select, TC context)
    {
        select.getFrom().accept(this, context);
        if (select.getWhere() != null)
        {
            visit(select.getWhere(), context);
        }
        select.getOrderBy().forEach(o -> o.accept(this, context));
        select.getGroupBy().forEach(e -> visit(e, context));
        select.getSelectItems().forEach(s -> s.accept(this, context));
        return null;
    }

    @Override
    public TR visit(TableSourceJoined joinedTableSource, TC context)
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
        nestedSelectItem.getOrderBy().forEach(si -> si.accept(this, context));
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
    public TR visit(SubQueryTableSource populatingJoin, TC context)
    {
        populatingJoin.getTableSourceJoined().accept(this, context);
        if (populatingJoin.getWhere() != null)
        {
            visit(populatingJoin.getWhere(), context);
        }
        populatingJoin.getGroupBy().forEach(e -> visit(e, context));
        populatingJoin.getOrderBy().forEach(o -> o.accept(this, context));
        return null;
    }

    @Override
    public TR visit(Join join, TC context)
    {
        join.getTableSource().accept(this, context);
        visit(join.getCondition(), context);
        return null;
    }

    @Override
    public TR visit(Apply apply, TC context)
    {
        apply.getTableSource().accept(this, context);
        return null;
    }
}
