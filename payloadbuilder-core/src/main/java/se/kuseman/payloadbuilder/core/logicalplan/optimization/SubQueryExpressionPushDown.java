package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/**
 * Rule the eliminates sub query expressions and move them so a nested loop in from clause.
 * 
 * <pre>
 * ie
 * 
 * select col,
 * (
 *    select *
 *    from values
 *    for objectarrray
 * ) values,
 * (
 *    select *
 *    from keys
 *    for objectarrray
 * ) keys,
 * 
 * from table a
 * 
 * Original plan
 * 
 *  projection (col, subquery values)
 *    scan: table a
 *    
 * Result plan:
 *  - Becuase all the sub queries are non correlated we have the FROM plan as the inner the outer will only return
 *    a single row
 * 
 *  projection (col, expr01 values, expr02 keys)
 *    nested loop (outer)  (expr01, expr02, a.*)
 *      nested loop (outer)   (expr01, expr02)
 *          TF: function: objectarray, output: expr01
 *              scan: values
 *          TF: function: objectarray, output: expr02
 *              scan: keys
 *      scan: table a
 * </pre>
 */
public class SubQueryExpressionPushDown extends ALogicalPlanOptimizer<SubQueryExpressionPushDown.Ctx>
{
    SubQueryExpressionPushDown()
    {
        super(SubQueryExpressionVisitor.INSTANCE);
    }

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        Ctx(IExecutionContext context)
        {
            super(context);
        }

        ILogicalPlan current;

        /** Queue with expression names that should be used as replacement for sub query expressions */
        Deque<String> aliasExpressionName = new ArrayDeque<>();
        SubQueryExpressionPushDown visitor;

        String getExpressionAlias()
        {
            if (aliasExpressionName.size() > 0)
            {
                return aliasExpressionName.pop();
            }
            return null;
        }
    }

    @Override
    Ctx createContext(IExecutionContext context)
    {
        return new Ctx(context);
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        ((Ctx) context).visitor = this;
        return plan.accept(this, (Ctx) context);
    }

    @Override
    protected ILogicalPlan create(ConstantScan plan, Ctx context)
    {
        ILogicalPlan prevCurrent = context.current;

        boolean allConstantScans = true;
        List<ILogicalPlan> logicalPlans = new ArrayList<>(plan.getRowsExpressions()
                .size());
        for (List<IExpression> columns : plan.getRowsExpressions())
        {
            context.current = ConstantScan.ONE_ROW_EMPTY_SCHEMA;
            List<IExpression> columnsResult = new ArrayList<>(columns.size());
            boolean projection = false;
            for (IExpression expression : columns)
            {
                IExpression result = expression.accept(SubQueryExpressionVisitor.INSTANCE, context);
                // Expression was rewritten => flag for a projection
                if (!expression.equals(result))
                {
                    projection = true;
                }
                columnsResult.add(result);
            }

            if (projection)
            {
                logicalPlans.add(new Projection(context.current, columnsResult, null));
                allConstantScans = false;
            }
            else
            {
                logicalPlans.add(plan.reCreate(List.of(columnsResult)));
            }
        }

        context.current = prevCurrent;

        // No pushdowns was made => return a constant scan with a rewritten result
        if (allConstantScans)
        {
            return plan.reCreate(logicalPlans.stream()
                    .map(l -> ((ConstantScan) l).getRowsExpressions()
                            .get(0))
                    .toList());
        }
        // .. or we had one row with a pushdown
        else if (logicalPlans.size() == 1)
        {
            return logicalPlans.get(0);
        }
        // .. else return a concatenation of all the plans generated
        return new Concatenation(plan.getSchema()
                .getColumns()
                .stream()
                .map(Column::getName)
                .toList(), logicalPlans, plan.getLocation());
    }

    @Override
    protected ILogicalPlan create(OperatorFunctionScan plan, Ctx context)
    {
        // Recreate operator function with a schema based on the context aliasExpressionName
        String alias = context.getExpressionAlias();

        assert (alias != null);

        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        Column column = plan.getSchema()
                .getColumns()
                .get(0);

        CoreColumn newCol = new CoreColumn(alias, column.getType(), null, true);
        Schema schema = Schema.of(newCol);
        return new OperatorFunctionScan(schema, input, plan.getCatalogAlias(), plan.getFunction(), plan.getLocation());
    }

    @Override
    protected ILogicalPlan create(Aggregate plan, Ctx context)
    {
        String alias = context.getExpressionAlias();
        ILogicalPlan prevCurrent = context.current;
        // Re create input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        context.current = input;

        // Not inside a sbuquery expression, recreate as usual
        if (alias == null)
        {
            List<IAggregateExpression> projections = plan.getProjectionExpressions()
                    .stream()
                    .map(e -> (IAggregateExpression) e.accept(SubQueryExpressionVisitor.INSTANCE, context))
                    .toList();
            ILogicalPlan result = new Aggregate(context.current, plan.getAggregateExpressions(), projections, plan.getParentTableSource());
            context.current = prevCurrent;
            return result;
        }

        int size = plan.getProjectionExpressions()
                .size();
        assert (size == 1) : "Projections in sub query expressions should only have 1 expression, got: " + plan.getProjectionExpressions();

        IExpression e = plan.getProjectionExpressions()
                .get(0);
        boolean singleValue = false;
        if (e instanceof AggregateWrapperExpression awe)
        {
            e = awe.getExpression();
            singleValue = awe.isSingleValue();
            if (e instanceof AliasExpression ae)
            {
                e = ae.getExpression();
            }
        }
        // Set the push down alias on the projection expression
        e = e.accept(SubQueryExpressionVisitor.INSTANCE, context);
        e = new AggregateWrapperExpression(new AliasExpression(e, alias, true), singleValue, true);

        ILogicalPlan result = context.current;
        context.current = prevCurrent;

        return new Aggregate(result, plan.getAggregateExpressions(), singletonList((IAggregateExpression) e), plan.getParentTableSource());
    }

    @Override
    protected ILogicalPlan create(Projection plan, Ctx context)
    {
        String alias = context.getExpressionAlias();
        ILogicalPlan prevCurrent = context.current;
        // Re create input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        context.current = input;

        // Not inside a sbuquery expression, recreate as usual
        if (alias == null)
        {
            List<IExpression> expressions = plan.getExpressions()
                    .stream()
                    .map(e -> e.accept(SubQueryExpressionVisitor.INSTANCE, context))
                    .toList();
            ILogicalPlan result = new Projection(context.current, expressions, plan.getParentTableSource());
            context.current = prevCurrent;
            return result;
        }

        int size = plan.getExpressions()
                .size();
        assert (size == 1) : "Projections in sub query expressions should only have 1 expression, got: " + plan.getExpressions();

        IExpression e = plan.getExpressions()
                .get(0);
        // Ditch the alias if any, not needed
        if (e instanceof AliasExpression ae)
        {
            e = ae.getExpression();
        }

        // Rewrite it and set the generated alias
        e = e.accept(SubQueryExpressionVisitor.INSTANCE, context);
        e = new AliasExpression(e, alias, true);

        ILogicalPlan result = context.current;
        context.current = prevCurrent;
        return new Projection(result, singletonList(e), plan.getParentTableSource());
    }

    /**
     * A expression rewriter for sub queries that extracts the sub queries to nested loops and replaces the expression with an alias expression calculated by the nested loop
     */
    private static class SubQueryExpressionVisitor extends ARewriteExpressionVisitor<Ctx>
    {
        private static final SubQueryExpressionVisitor INSTANCE = new SubQueryExpressionVisitor();

        @Override
        public IExpression visit(UnresolvedFunctionCallExpression expression, Ctx context)
        {
            throw new IllegalArgumentException("UnresolvedFunctionCallExpression should not be present at this stage");
        }

        @Override
        public IExpression visit(UnresolvedSubQueryExpression expression, Ctx ctx)
        {
            // A constant scan with one row one column -> simply return the single column expression
            // This is a subquery like "(select column)" => column
            if (expression.getInput() instanceof ConstantScan cs
                    && cs.getRowsExpressions()
                            .size() == 1
                    && cs.getRowsExpressions()
                            .get(0)
                            .size() == 1)
            {
                IExpression e = cs.getRowsExpressions()
                        .get(0)
                        .get(0);

                // Unnest any aliases at this stage since they are never used
                if (e instanceof AliasExpression ae)
                {
                    e = ae.getExpression();
                }

                return visit(e, ctx);
            }

            String alias = "__expr" + ctx.expressionCounter++;

            // TODO: There might be a schema column named exactly like the generated one
            ctx.aliasExpressionName.push(alias);

            // Visit the sub expressions plan
            ILogicalPlan plan = expression.getInput()
                    .accept(ctx.visitor, ctx);

            // We have a non operator function here then we need to make sure that the query
            // don't return more than one row since a sub query expression is a scalar expression
            if (wrapInAssert(expression))
            {
                boolean has1Limit = (plan instanceof Limit l
                        && l.getLimitExpression() instanceof LiteralIntegerExpression lie
                        && lie.getValue() <= 1);

                // If we have a limit plan with a literal 1 then we don't need an assert
                if (!has1Limit)
                {
                    plan = new MaxRowCountAssert(plan, 1);
                }
            }

            // If plan is guaranteed to return at least one row then no join is needed
            if (ConstantScan.ONE_ROW_EMPTY_SCHEMA.equals(ctx.current)
                    && returnsAtLeastOneRow(plan))
            {
                ctx.current = plan;
            }
            else
            {
                ILogicalPlan left = ctx.current;
                ILogicalPlan right = plan;

                ctx.current = new Join(left, right, Join.Type.LEFT, null, (IExpression) null, emptySet(), false, Schema.EMPTY);
            }
            // Return an unresolved column that will be resolved as an ordinary column at next stage (ColumnResolver)
            return new UnresolvedColumnExpression(QualifiedName.of(alias), -1, expression.getLocation());
        }

        private boolean returnsAtLeastOneRow(ILogicalPlan plan)
        {
            if (plan instanceof OperatorFunctionScan fs
                    && fs.getInput() instanceof ConstantScan cs
                    && cs.getRowsExpressions()
                            .size() >= 1)
            {
                return true;
            }
            return false;
        }

        private boolean wrapInAssert(UnresolvedSubQueryExpression expression)
        {
            // Operator function scans always return one row
            if (expression.getInput() instanceof OperatorFunctionScan)
            {
                return false;
            }

            ILogicalPlan plan = expression.getInput();
            if (plan instanceof Projection p)
            {
                plan = p.getInput();
            }
            if (plan instanceof SubQuery s)
            {
                plan = s.getInput();
            }

            if (plan instanceof ConstantScan cs)
            {
                if (cs.getRowsExpressions()
                        .size() == 1)
                {
                    return false;
                }
                else if (cs.getRowsExpressions()
                        .size() > 1)
                {
                    throw new ParseException("A sub query expression must return a single row", cs.getLocation());
                }
            }

            return true;
        }
    }
}
