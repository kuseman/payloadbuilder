package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptySet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
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

        Schema outerSchema;

        ILogicalPlan current;

        /** Queue with expression names that should be used as replacement for sub query expressions */
        Deque<String> aliasExpressionName = new ArrayDeque<>();

        /**
         * Ordinal in projection schema for the current processing sub query expression. If the schema where the expression is participating is NOT asterisk we can access the generated column by
         * ordinal to avoid column name lookup runtime.
         */
        int subQueryExpressionOrdinal = -1;
        boolean inputSchemaAsterisk = false;

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

        Schema schema = Schema.of(SchemaUtils.rename(plan.getSchema()
                .getColumns()
                .get(0), alias));

        return new OperatorFunctionScan(schema, input, plan.getCatalogAlias(), plan.getFunction(), plan.getLocation());
    }

    @Override
    protected ILogicalPlan create(Aggregate plan, Ctx context)
    {
        // Not inside a sbuquery expression, recreate as usual
        String alias = context.getExpressionAlias();
        if (alias == null)
        {
            return super.create(plan, context);
        }

        // This is an aggregate inside a subquery expression, set alias on the projection
        // Re create input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        int size = plan.getProjectionExpressions()
                .size();
        List<IAggregateExpression> projectionExpressions = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            IExpression e = plan.getProjectionExpressions()
                    .get(i);

            // Set alias
            // NOTE! A aggregate inside a sub query expression can only have one projection column
            if (i == 0)
            {
                boolean singleValue = false;
                boolean internal = false;
                if (e instanceof AggregateWrapperExpression awe)
                {
                    e = awe.getExpression();
                    singleValue = awe.isSingleValue();
                    internal = awe.isInternal();
                    if (e instanceof AliasExpression ae)
                    {
                        e = ae.getExpression();
                    }
                }
                e = new AggregateWrapperExpression(new AliasExpression(e, alias), singleValue, internal);
            }
            projectionExpressions.add((IAggregateExpression) e);
        }

        return new Aggregate(input, plan.getAggregateExpressions(), projectionExpressions);
    }

    @Override
    protected ILogicalPlan create(Projection plan, Ctx context)
    {
        // Re create input
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // CSOFF
        Schema prevOuterSchema = context.outerSchema;
        boolean prevInputSchemaAsterisk = context.inputSchemaAsterisk;
        ILogicalPlan prevCurrent = context.current;
        // CSON
        context.current = input;
        context.outerSchema = SchemaUtils.joinSchema(context.outerSchema, input.getSchema());

        int prevSubQueryExpressionOrdinal = context.subQueryExpressionOrdinal;

        // Check if the projection is asterisk or not. Used to determine if we can use ordinals
        // or not for sub query expression substitute columns.
        Schema inputSchema = input.getSchema();
        int inputSchemaSize = inputSchema.getSize();
        context.inputSchemaAsterisk = inputSchemaSize > 0
                && SchemaUtils.isAsterisk(inputSchema, false);

        // This the start ordinal for where pushed down sub query expressions will be
        /*
         * @formatter:off
         *
         * Schema tableA: [col, col1, col2]
         *
         * Before pushdown:
         *
         * select col
         * ,      ( subquery1 )
         * ,      ( subquery2 )
         * from tableA
         *
         * After pushdown:
         *
         * select col
         * ,      __expr0   (Ordinal 3 + 0 = 3)
         * ,      __expr1   (Ordinal 3 + 1 = 4)
         * from tableA
         * left join (subquery1)
         * left join (subquery2)
         *
         * Tree:
         *
         * join                     Output: (col, col1, col2, __expr0, __expr1)
         *   join                   Output: (col, col1, col2, __expr0)
         *     scan: table          Output: (col, col1, col2)
         *     (subquery1 join)     Output: (__expr0)
         *   (subquery2 join)       Output: (__expr1)
         *
         * @formatter:on
         */
        context.subQueryExpressionOrdinal = inputSchemaSize;

        // Recreate sub query expressions
        int size = plan.getExpressions()
                .size();
        List<IExpression> expressions = new ArrayList<>(size);

        String alias = context.getExpressionAlias();

        ILogicalPlan current = context.current;

        for (int i = 0; i < size; i++)
        {
            IExpression e = plan.getExpressions()
                    .get(i);

            e = e.accept(SubQueryExpressionVisitor.INSTANCE, context);

            // Alter the outer schema along the way when we introduce new join operators in the tree
            if (context.current != current)
            {
                context.outerSchema = SchemaUtils.joinSchema(prevOuterSchema, context.current.getSchema());
                current = context.current;
            }

            // Project the expression from previous expression rewrite
            // NOTE! This is only done for the first projection expression
            // since that is the one we are "pushing down"
            // there can be other projection expressions, for example
            // an internal order by column that should not get aliased
            if (alias != null
                    && i == 0)
            {
                e = new AliasExpression(e, alias);
            }

            expressions.add(e);
        }

        // We have nested projections, combine these
        if (context.current instanceof Projection p)
        {
            // Rewrite this projection expressions with the inner
            expressions = ProjectionMerger.replace(expressions, p.getExpressions());
            // ... and remove the inner projection
            context.current = p.getInput();
        }
        else if (context.current instanceof OperatorFunctionScan ofs)
        {
            // Remove this projection since the child is a operator function which already has correct schema
            // just change it's name
            if (alias != null
                    && size == 1)
            {
                // Restore previous context values
                context.current = prevCurrent;
                context.subQueryExpressionOrdinal = prevSubQueryExpressionOrdinal;
                context.outerSchema = prevOuterSchema;
                context.inputSchemaAsterisk = prevInputSchemaAsterisk;

                Schema schema = Schema.of(SchemaUtils.rename(ofs.getSchema()
                        .getColumns()
                        .get(0), alias));
                return new OperatorFunctionScan(schema, ofs.getInput(), ofs.getCatalogAlias(), ofs.getFunction(), ofs.getLocation());
            }
        }

        // CSOFF
        ILogicalPlan result = new Projection(context.current, expressions);
        // CSON

        // Restore previous context values
        context.current = prevCurrent;
        context.subQueryExpressionOrdinal = prevSubQueryExpressionOrdinal;
        context.outerSchema = prevOuterSchema;
        context.inputSchemaAsterisk = prevInputSchemaAsterisk;
        return result;
    }

    /**
     * A expression rewriter for sub queries that extracts the sub queries to nested loops and replaces the expression with an alias expression calculated by the nested loop
     */
    private static class SubQueryExpressionVisitor extends ARewriteExpressionVisitor<Ctx>
    {
        private static final SubQueryExpressionVisitor INSTANCE = new SubQueryExpressionVisitor();

        @Override
        public IExpression visit(UnresolvedSubQueryExpression expression, Ctx ctx)
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
                    boolean addAssert = true;

                    // If we have a limit plan with a literal 1 then we don't need an assert
                    if (plan instanceof Limit)
                    {
                        Limit limit = (Limit) plan;
                        if (limit.getLimitExpression() instanceof LiteralIntegerExpression
                                && ((LiteralIntegerExpression) limit.getLimitExpression()).getValue() <= 1)
                        {
                            addAssert = false;
                        }
                    }

                    if (addAssert)
                    {
                        plan = new MaxRowCountAssert(plan, 1);
                    }
                }

                /*
                 * @formatter:off
                 * 
                 * if we have a non-correlated sub query then we put the plan as outer in a nested loop (left) because
                 * then that will only execute once BUT we must make sure that the query never circuit breaks
                 * the main query by returning zero rows (that will be catastrophic because the main query will be empty)
                 * so we nest it in another nested loop (left)
                 * with a constant scan as outer, that way we are guaranteed that we always end up with at least one row.
                 * 
                 * nested loop (left)
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

                // Nest plan in another nested loop to guarantee at least one row
                if (!correlated)
                {
                    left = new Join(ConstantScan.INSTANCE, left, Join.Type.LEFT, null, (IExpression) null, emptySet(), false, Schema.EMPTY);
                }

                ctx.current = new Join(left, right, Join.Type.LEFT, null, (IExpression) null, expression.getOuterReferences(), !correlated, ctx.outerSchema);
            }

            // A sub query plan can only have a single column (either an OperatorFunction or a single column projection)
            Column column = plan.getSchema()
                    .getColumns()
                    .get(0);

            ResolvedType type = column.getType();
            ColumnExpression.Builder builder = ColumnExpression.Builder.of(alias, type);

            TableSourceReference tableSource = SchemaUtils.getTableSource(column);
            if (tableSource != null)
            {
                CoreColumn.Type columnType = SchemaUtils.getColumnType(column);
                builder.withColumnReference(new ColumnReference(tableSource, columnType));
            }
            if (!ctx.inputSchemaAsterisk)
            {
                builder.withOrdinal(ctx.subQueryExpressionOrdinal++);
            }
            else
            {
                builder.withColumn(alias);
            }

            // Return an alias expression for the current sub query
            return builder.build();
        }
    }
}
