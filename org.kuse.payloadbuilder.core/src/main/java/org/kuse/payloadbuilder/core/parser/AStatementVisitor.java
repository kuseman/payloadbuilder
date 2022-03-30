package org.kuse.payloadbuilder.core.parser;

import java.util.List;

/**
 * Visitor adapter for expression visitors
 *
 * @param <TR> Result type
 * @param <TC> Context type
 */
public class AStatementVisitor<TR, TC> implements StatementVisitor<TR, TC>, SelectVisitor<TR, TC>
{
    private static final Statement[] EMPTY = new Statement[0];

    /**
     * Default result
     *
     * @param context Context for visitor
     **/
    protected TR defaultResult(TC context)
    {
        return null;
    }

    /**
     * Visit expression contained in a statement
     *
     * @param context Context
     * @param expression Expression
     **/
    protected void visitExpression(TC context, Expression expression)
    {
    }

    /** Aggregate results */
    @SuppressWarnings("unused")
    protected TR aggregate(TR result, TR nextResult)
    {
        return nextResult;
    }

    @Override
    public TR visit(IfStatement statement, TC context)
    {
        visitExpression(context, statement.getCondition());
        return visitChildren(context, statement.getStatements(), statement.getElseStatements().toArray(EMPTY));
    }

    @Override
    public TR visit(PrintStatement statement, TC context)
    {
        visitExpression(context, statement.getExpression());
        return null;
    }

    @Override
    public TR visit(SetStatement statement, TC context)
    {
        visitExpression(context, statement.getExpression());
        return null;
    }

    @Override
    public TR visit(UseStatement statement, TC context)
    {
        if (statement.getExpression() != null)
        {
            visitExpression(context, statement.getExpression());
        }
        return null;
    }

    @Override
    public TR visit(DescribeTableStatement statement, TC context)
    {
        return null;
    }

    @Override
    public TR visit(DropTableStatement statement, TC context)
    {
        return null;
    }

    @Override
    public TR visit(DescribeSelectStatement statement, TC context)
    {
        statement.getSelectStatement().getSelect().accept(this, context);
        return null;
    }

    @Override
    public TR visit(AnalyzeStatement statement, TC context)
    {
        statement.getSelectStatement().getSelect().accept(this, context);
        return null;
    }

    @Override
    public TR visit(ShowStatement statement, TC context)
    {
        return null;
    }

    @Override
    public TR visit(CacheFlushRemoveStatement statement, TC context)
    {
        visitExpression(context, statement.getKey());
        return null;
    }

    @Override
    public TR visit(SelectStatement statement, TC context)
    {
        statement.getSelect().accept(this, context);
        return null;
    }

    /* SELECT visitor */

    @Override
    public TR visit(Select select, TC context)
    {
        select.getSelectItems().forEach(s -> s.accept(this, context));
        select.getComputedExpressions().forEach(e -> visitExpression(context, e));
        if (select.getFrom() != null)
        {
            select.getFrom().accept(this, context);
        }
        if (select.getWhere() != null)
        {
            visitExpression(context, select.getWhere());
        }
        if (select.getTopExpression() != null)
        {
            visitExpression(context, select.getTopExpression());
        }
        select.getGroupBy().forEach(g -> visitExpression(context, g));
        select.getOrderBy().forEach(o -> o.accept(this, context));
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
        visitExpression(context, sortItem.getExpression());
        return null;
    }

    @Override
    public TR visit(ExpressionSelectItem expressionSelectItem, TC context)
    {
        visitExpression(context, expressionSelectItem.getExpression());
        return null;
    }

    @Override
    public TR visit(AsteriskSelectItem selectItem, TC context)
    {
        return null;
    }

    @Override
    public TR visit(Table table, TC context)
    {
        table.getOptions().forEach(to -> visitExpression(context, to.getValueExpression()));
        return null;
    }

    @Override
    public TR visit(UnresolvedTableFunction tableFunction, TC context)
    {
        tableFunction.getArguments().forEach(a -> visitExpression(context, a));
        tableFunction.getOptions().forEach(to -> visitExpression(context, to.getValueExpression()));
        return null;
    }

    @Override
    public TR visit(TableFunction tableFunction, TC context)
    {
        tableFunction.getArguments().forEach(a -> visitExpression(context, a));
        tableFunction.getOptions().forEach(to -> visitExpression(context, to.getValueExpression()));
        return null;
    }

    @Override
    public TR visit(Join join, TC context)
    {
        join.getTableSource().accept(this, context);
        visitExpression(context, join.getCondition());
        return null;
    }

    @Override
    public TR visit(Apply apply, TC context)
    {
        apply.getTableSource().accept(this, context);
        return null;
    }

    @Override
    public TR visit(SubQueryTableSource tableSource, TC context)
    {
        tableSource.getSelect().accept(this, context);
        tableSource.getOptions().forEach(to -> visitExpression(context, to.getValueExpression()));
        return null;
    }

    /* END SELECT visitor */

    /** Visit provided children */
    protected TR visitChildren(TC context, List<Statement> statements, Statement... args)
    {
        TR result = defaultResult(context);
        if (statements != null)
        {
            for (Statement s : statements)
            {
                TR value = s.accept(this, context);
                result = aggregate(result, value);
            }
        }

        if (args != null)
        {
            for (Statement s : args)
            {
                TR value = s.accept(this, context);
                result = aggregate(result, value);
            }
        }
        return result;
    }
}
