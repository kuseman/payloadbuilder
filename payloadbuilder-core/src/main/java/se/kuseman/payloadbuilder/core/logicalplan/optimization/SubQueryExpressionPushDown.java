package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.SubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;

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
    protected ILogicalPlan create(OperatorFunctionScan plan, Ctx context)
    {
        // Recreate operator function with a schema based on the context aliasExpressionName
        String alias = context.getExpressionAlias();

        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        Schema schema = Schema.of(Column.of(defaultIfBlank(alias, ""), plan.getSchema()
                .getColumns()
                .get(0)
                .getType()));

        return new OperatorFunctionScan(schema, input, plan.getCatalogAlias(), plan.getFunction(), plan.getToken());
    }

    @Override
    protected ILogicalPlan create(Projection plan, Ctx context)
    {
        // Re create input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        ILogicalPlan prevCurrent = context.current;
        context.current = input;

        // Recreate sub query expressions
        int size = plan.getExpressions()
                .size();
        List<IExpression> expressions = new ArrayList<>(size);

        String alias = context.getExpressionAlias();

        for (int i = 0; i < size; i++)
        {
            IExpression e = plan.getExpressions()
                    .get(i);

            e = e.accept(SubQueryExpressionVisitor.INSTANCE, context);

            // Project the expression from previous expression rewrite
            if (alias != null)
            {
                e = new AliasExpression(e, alias);
            }

            expressions.add(e);
        }

        // We have nested projections, combine these
        if (context.current instanceof Projection)
        {
            // Rewrite this projection expressions with the inner
            expressions = ProjectionMerger.replace(expressions, (Projection) context.current);
            // ... and remove the inner projection
            context.current = ((Projection) context.current).getInput();
        }
        else if (context.current instanceof OperatorFunctionScan)
        {
            // Remove this projection since the child is a tuple function which already has correct schema
            // just change it's name
            if (alias != null
                    && size == 1)
            {
                OperatorFunctionScan tf = (OperatorFunctionScan) context.current;
                Schema schema = Schema.of(Column.of(alias, tf.getSchema()
                        .getColumns()
                        .get(0)
                        .getType()));

                // Restore previous current plan
                context.current = prevCurrent;

                return new OperatorFunctionScan(schema, tf.getInput(), tf.getCatalogAlias(), tf.getFunction(), tf.getToken());
            }
        }

        ILogicalPlan result = new Projection(context.current, expressions, plan.isAppendInputColumns());

        // Restore previous current plan
        context.current = prevCurrent;
        return result;
    }

    /**
     * A expression rewriter for sub queries that extracts the sub queries to nested loops and replaces the expression with an alias expression calculated by the nested loop
     */
    private static class SubQueryExpressionVisitor extends ARewriteExpressionVisitor<Ctx>
    {
        private static final SubQueryExpressionVisitor INSTANCE = new SubQueryExpressionVisitor();

        @Override
        public IExpression visit(SubQueryExpression expression, Ctx ctx)
        {
            String alias = "__expr" + ctx.expressionCounter++;

            ctx.aliasExpressionName.push(alias);

            // Visit the sub expressions plan
            ILogicalPlan plan = expression.getInput()
                    .accept(ctx.visitor, ctx);

            // The current plan is a constant scan, ie. a nested sub query expression with
            // out a table source, then we unwrap this and don't create a join
            if (ctx.current instanceof ConstantScan)
            {
                ctx.current = plan;
            }
            else
            {
                // We have a non operator function here then we need to make sure that the query
                // don't return more than one row since a sub query expression is a scalar expression
                // If the plan is a OperatorFunctionScan it always return one row
                if (!(plan instanceof OperatorFunctionScan))
                {
                    plan = new MaxRowCountAssert(plan, 1);
                }

                /*
                 * @formatter:off
                 * 
                 * if we have a non-correlated sub query then we put the plan as outer in a nested loop (inner) because
                 * then that will only execute once BUT we must make sure that the query never circuit breaks
                 * the main query by returning zero rows (that will be catastrophic because the main query will be empty)
                 * so we nest it in another nested loop (left)
                 * with a constant scan as outer, that way we are guaranteed that we always end up with at least one row.
                 * 
                 * nested loop (inner)
                 *   nested loop (left)             <-- Will always return at least 1 row
                 *     constant scan
                 *     sub query plan
                 *   current plan 
                 * 
                 * if we have a correlated sub query we simply put a nested loop with plan as  inner since the existing plan will be outer.
                 * If we are on top it's safe because if that returns 0 rows we will get 0 rows in the main query. if it's a nested
                 * sub query we are also safe because of previous bullet
                 * 
                 * nested loop (left)
                 *   current plan
                 *   sub query plan
                 *   
                 * 
                 * @formatter:on
                 */

                boolean correlated = !expression.getOuterReferences()
                        .isEmpty();

                // If the sub query is not correlated we put the sub query as the outer in the join
                // this because we only want to execute that once since it doesn't change
                // We also need to flag for schema switch to stay consistent
                ILogicalPlan left = correlated ? ctx.current
                        : plan;
                ILogicalPlan right = correlated ? plan
                        : ctx.current;

                // Set the top join type based on correlation
                Join.Type joinType = correlated ? Join.Type.LEFT
                        : Join.Type.INNER;

                // Nest plan in another nested loop to guarantee at least one row
                if (!correlated)
                {
                    left = new Join(ConstantScan.INSTANCE, left, Join.Type.LEFT, null, (IExpression) null, emptySet(), false);
                }

                ctx.current = new Join(left, right, joinType, null, (IExpression) null, expression.getOuterReferences(), !correlated);
            }

            // A sub query plan can only have a single column (either an OperatorFunction or a single column projection)
            ResolvedType type = plan.getSchema()
                    .getColumns()
                    .get(0)
                    .getType();

            // TODO: find out if the current schema is static and we can access the exprX column by index
            // Return an alias expression for the current sub query
            return ColumnExpression.Builder.of(alias, type)
                    .withColumn(alias)
                    .build();
        }
    }
}
