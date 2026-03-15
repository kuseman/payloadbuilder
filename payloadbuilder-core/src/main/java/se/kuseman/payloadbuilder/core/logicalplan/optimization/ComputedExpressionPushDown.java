package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.Strings.CI;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/**
 * Optimizer that pushes down computed expressions that is used in both projection/sort/aggregations.
 */
class ComputedExpressionPushDown extends ALogicalPlanOptimizer<ComputedExpressionPushDown.Ctx>
{
    private static final String ORDER_BY_CONSTANT_ENCOUNTERED = "ORDER BY constant encountered";

    ComputedExpressionPushDown()
    {
        super(null);
    }

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        Ctx(IExecutionContext context)
        {
            super(context);
        }

        Deque<PlanData> planDatas = new ArrayDeque<>();
        ComputedExpressionPushDown visitor;
    }

    static class PlanData
    {
        /**
         * List of sort expressions used if we pass a sort plan and enter an aggregate to see if we should add an aggregate expression as projection to be able sort on it
         */
        List<SortItem> sortItems = emptyList();

        /** Filter (Having) that will be processed by group by clause */
        IExpression filterPredicate;
    }

    @Override
    Ctx createContext(IExecutionContext context)
    {
        Ctx ctx = new Ctx(context);
        ctx.visitor = this;
        return ctx;
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        Ctx ctx = (Ctx) context;
        ctx.planDatas.push(new PlanData());
        return plan.accept(this, ctx);
    }

    @Override
    public ILogicalPlan visit(SubQuery plan, Ctx context)
    {
        // When starting a sub query we need a new projection scope to not thrash current data
        context.planDatas.push(new PlanData());
        ILogicalPlan result = super.visit(plan, context);
        context.planDatas.pop();
        return result;
    }

    @Override
    public ILogicalPlan visit(Sort plan, Ctx context)
    {
        PlanData planData = context.planDatas.peek();
        planData.sortItems = new ArrayList<>(plan.getSortItems());
        ILogicalPlan sortInput = plan.getInput()
                .accept(this, context);
        List<SortItem> result = planData.sortItems;
        planData.sortItems = emptyList();
        return new Sort(sortInput, result);
    }

    @Override
    public ILogicalPlan visit(ConstantScan plan, Ctx context)
    {
        if (Schema.EMPTY.equals(plan.getSchema())
                || plan.getRowsExpressions()
                        .isEmpty())
        {
            return super.create(plan, context);
        }

        List<List<IExpression>> rowsExpressions = plan.getRowsExpressions()
                .stream()
                .map(list -> list.stream()
                        .map(e -> UnresolvedSubQueryExpressionVisitor.INSTANCE.visit(e, context))
                        .toList())
                .toList();

        return plan.reCreate(rowsExpressions);
    }

    @Override
    public ILogicalPlan visit(Projection plan, Ctx context)
    {
        // Skip asterisk projections
        if (plan.isAsteriskProjection())
        {
            return super.visit(plan, context);
        }

        PlanData planData = context.planDatas.peek();

        Map<String, Pair<Integer, QualifiedName>> projectionExpressionByName = new HashMap<>();

        int size = plan.getExpressions()
                .size();
        List<IExpression> projectionExpressions = new ArrayList<>(size);
        int firstAsteriskIndex = -1;
        for (int i = 0; i < size; i++)
        {
            IExpression e = plan.getExpressions()
                    .get(i);
            if (e instanceof AsteriskExpression
                    && firstAsteriskIndex == -1)
            {
                // One based since order by ordinal is one based
                firstAsteriskIndex = i + 1;
            }

            // Index all named projections, this to be able to find out if we need to add a projection that is sorted on and is not projected
            if (e instanceof UnresolvedColumnExpression ce)
            {
                projectionExpressionByName.put(ce.getAlias()
                        .getAlias()
                        .toLowerCase(), Pair.of(i, ce.getColumn()));
            }
            else if (e instanceof HasAlias alias)
            {
                projectionExpressionByName.put(alias.getAlias()
                        .getAlias()
                        .toLowerCase(), Pair.of(i, null));
            }

            // Traverse projection expression and compute any sub queries
            projectionExpressions.add(UnresolvedSubQueryExpressionVisitor.INSTANCE.visit(e, context));
        }

        // Nothing more to do, return new plan with rewritten expressions
        if (planData.sortItems.isEmpty())
        {
            return new Projection(plan.getInput()
                    .accept(this, context), projectionExpressions, plan.getParentTableSource());
        }

        List<IExpression> newProjectionExpressions = new ArrayList<>(projectionExpressions);

        // Search sort items for rewrite
        // - Ordinal sorts (ORDER BY 1)
        // - Sorts on columns not projected => add to projection as internal
        int sortItemSize = planData.sortItems.size();
        for (int j = 0; j < sortItemSize; j++)
        {
            SortItem item = planData.sortItems.get(j);
            IExpression itemExpression = item.getExpression();

            int projectionExpressionIndex = -1;
            // Order by ordinal
            if (itemExpression instanceof LiteralIntegerExpression)
            {
                int index = ((LiteralIntegerExpression) itemExpression).getValue();

                if (index <= 0
                        || (firstAsteriskIndex == -1
                                && index > projectionExpressions.size()))
                {
                    throw new ParseException("ORDER BY position is out of range", item.getLocation());
                }
                // There are asterisks before or on the index then we sort by the ordinal runtime
                else if (firstAsteriskIndex >= 0
                        && index >= firstAsteriskIndex)
                {
                    continue;
                }

                projectionExpressionIndex = index - 1;
            }
            else
            {
                // See if the items expression is semantic equal to a projected expression
                for (int i = 0; i < size; i++)
                {
                    IExpression projExpression = projectionExpressions.get(i);
                    if (projExpression instanceof AliasExpression ae)
                    {
                        projExpression = ae.getExpression();
                    }

                    if (itemExpression.semanticEquals(projExpression))
                    {
                        projectionExpressionIndex = i;
                        break;
                    }
                }
            }

            // If we still don't have found a projection expression that matches
            // find all column references and and internal ones for those missing
            if (projectionExpressionIndex == -1)
            {
                // Gather all referenced columns from order by expression
                Set<UnresolvedColumnExpression> columnExpressions = new HashSet<>();
                item.getExpression()
                        .accept(UnresolvedColumnCollector.INSTANCE, columnExpressions);

                for (UnresolvedColumnExpression colExpression : columnExpressions)
                {
                    String alias = colExpression.getColumn()
                            .getAlias();
                    String column = colExpression.getAlias()
                            .getAlias();

                    Pair<Integer, QualifiedName> pair = projectionExpressionByName.get(column.toLowerCase());

                    /* @formatter:off
                     *
                     * select col
                     * from table
                     * order by col2                 <-- col2 is not projected, add it to projections as internal
                     *
                     * @formatter:on
                     */
                    // Add an interal projection for missing column
                    if (pair == null)
                    {
                        newProjectionExpressions.add(new AliasExpression(colExpression, column, true));
                        continue;
                    }

                    // If we found a match then make sure they references the same alias, if not add an internal projection
                    // order by: d.col
                    // projection: b.col
                    QualifiedName qname = pair.getValue();
                    if (qname != null
                            && alias != null
                            && !alias.equalsIgnoreCase(qname.getAlias()))
                    {
                        newProjectionExpressions.add(new AliasExpression(colExpression, column, true));
                        continue;
                    }

                    // Verify constant sort
                    if (projectionExpressions.get(pair.getKey())
                            .isConstant())
                    {
                        throw new ParseException(ORDER_BY_CONSTANT_ENCOUNTERED, item.getLocation());
                    }
                }
            }

            if (projectionExpressionIndex >= 0)
            {
                IExpression projectionExpression = projectionExpressions.get(projectionExpressionIndex);

                // Sorting on a constant => error
                if (projectionExpression.isConstant())
                {
                    throw new ParseException(ORDER_BY_CONSTANT_ENCOUNTERED, item.getLocation());
                }

                if (!shouldReplaceSortItem(itemExpression, projectionExpression))
                {
                    continue;
                }

                String alias = projectionExpression instanceof HasAlias ha ? ha.getAlias()
                        .getAlias()
                        : null;

                // Simply replace the ordinal with the projection expression if a column
                if (projectionExpression instanceof UnresolvedColumnExpression)
                {
                    planData.sortItems.set(j, new SortItem(projectionExpression, item.getOrder(), item.getNullOrder(), item.getLocation()));
                }
                // Projected expression has an alias, point the sort item to the alias
                else if (!isBlank(alias))
                {
                    // Point the sort item to the projected alias
                    planData.sortItems.set(j, new SortItem(new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null), item.getOrder(), item.getNullOrder(), item.getLocation()));
                }
                // Computed projection expression, push down
                else if (isComputed(projectionExpression))
                {
                    alias = "__expr" + context.expressionCounter++;

                    // Replace the projection expression with an alias expression with generated name
                    IExpression newProjectionExpression = new AliasExpression(projectionExpression, alias, projectionExpression.toString(), false);
                    newProjectionExpressions.set(projectionExpressionIndex, newProjectionExpression);

                    planData.sortItems.set(j, new SortItem(new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null), item.getOrder(), item.getNullOrder(), item.getLocation()));
                }
            }
        }

        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        return new Projection(input, newProjectionExpressions, plan.getParentTableSource());
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        PlanData planData = context.planDatas.peek();

        // Having
        if (plan.getInput() instanceof Aggregate)
        {
            planData.filterPredicate = plan.getPredicate();
            ILogicalPlan input = plan.getInput()
                    .accept(this, context);

            IExpression filterPredicate = planData.filterPredicate;
            planData.filterPredicate = null;

            // filterPredicate may have been consumed by visit(Aggregate) when mixed expressions
            // required placing the Filter between the Aggregate and the outer Projection
            if (filterPredicate == null)
            {
                return input;
            }

            return new Filter(input, plan.getTableSource(), filterPredicate);
        }

        return super.create(plan, context);
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
        // Distinct should not be processed here
        if (plan.getProjectionExpressions()
                .isEmpty())
        {
            return plan;
        }

        // CSOFF
        PlanData planData = context.planDatas.peek();
        // CSON

        int projectionSize = plan.getProjectionExpressions()
                .size();

        // Named projection columns ie. alias expressions
        Set<String> projectionColumns = new HashSet<>();
        List<IExpression> unWrappedPlanProjections = new ArrayList<>(projectionSize);
        List<IAggregateExpression> planProjections = new ArrayList<>(projectionSize);
        for (int i = 0; i < projectionSize; i++)
        {
            IExpression e = plan.getProjectionExpressions()
                    .get(i);
            planProjections.add((IAggregateExpression) e);

            // Unwrap aggregate and alias if any
            if (e instanceof AggregateWrapperExpression)
            {
                e = ((AggregateWrapperExpression) e).getExpression();
            }

            // The alias is not needed down here, it will be re-added on the top projection upon return
            if (e instanceof HasAlias)
            {
                HasAlias.Alias alias = ((HasAlias) e).getAlias();
                if (!StringUtils.isBlank(alias.getAlias()))
                {
                    projectionColumns.add(alias.getAlias()
                            .toLowerCase());
                }
            }
            if (e instanceof AliasExpression)
            {
                e = ((AliasExpression) e).getExpression();
            }

            unWrappedPlanProjections.add(e);
        }

        List<Pair<String, IExpression>> pushDownExpressions = new ArrayList<>();
        AggregateColumnsCollector.Context ctx = new AggregateColumnsCollector.Context(context, planProjections, pushDownExpressions);

        /* @formatter:off
         *
         * ANSI SQL compliance: aggregate functions should only appear inside the Aggregate operator.
         * Any expression that combines aggregate function results with non-aggregate operations
         * (e.g. t.col + max(col1), count(*) * 2) must be split:
         *   - The aggregate function(s) are extracted and kept in the Aggregate with generated aliases
         *   - The outer computation is placed in a Projection operator above the Aggregate
         *
         * Example:  SELECT count(1), t.col + max(col1) FROM t GROUP BY t.col
         *   Before:  Aggregate(projections: [count(1), AggWrapper(t.col + max(col1))])
         *   After:   Projection([__expr0, t.col + __expr1],
         *              Aggregate(projections: [AggWrapper(count(1) as __expr0), AggWrapper(max(col1) as __expr1 internal), AggWrapper(t.col internal)]))
         *
         * @formatter:on
         */
        List<IExpression> outerProjectionExpressions = null;
        for (int i = 0; i < projectionSize; i++)
        {
            IAggregateExpression pe = planProjections.get(i);
            if (!isMixedProjection(pe))
            {
                // Non-mixed: if we already have outer projections, add a pass-through reference for this projection
                if (outerProjectionExpressions != null)
                {
                    outerProjectionExpressions.add(getAggregateProjectionRef(pe, i, planProjections, ctx));
                }
                continue;
            }

            // Mixed projection found: initialize outer projections if not done yet
            if (outerProjectionExpressions == null)
            {
                outerProjectionExpressions = new ArrayList<>();
                // Add pass-through references for all previously processed non-mixed projections
                for (int j = 0; j < i; j++)
                {
                    outerProjectionExpressions.add(getAggregateProjectionRef(planProjections.get(j), j, planProjections, ctx));
                }
            }

            // Unwrap the mixed expression (strip AggregateWrapper and optional AliasExpression)
            IExpression inner = ((AggregateWrapperExpression) pe).getExpression();
            String alias = null;
            if (inner instanceof AliasExpression ae)
            {
                alias = ae.getAliasString();
                inner = ae.getExpression();
            }

            // Extract aggregate functions from the mixed expression via AggregateColumnsCollector.
            // This adds internal projections for each aggregate function found and returns
            // a rewritten expression that uses column references instead of aggregate function calls.
            ctx.nonAggegatedColumnExpressions.clear();
            IExpression transformed = inner.accept(AggregateColumnsCollector.INSTANCE, ctx);
            validateExpressions(plan, ctx, projectionColumns, unWrappedPlanProjections, planProjections, false, "SELECT");

            // Build the outer projection expression, preserving the alias if there was one.
            // If no alias and the mixed expression was a bare non-aggregate function call
            // (wrapped by SchemaResolver so it could reach this path), use the original
            // expression's toString() as the column name for a user-readable output column.
            // For other no-alias mixed expressions (e.g. arithmetic), keep the old behavior.
            IExpression outerExpr;
            if (alias != null)
            {
                outerExpr = new AliasExpression(transformed, alias);
            }
            else if (inner instanceof IFunctionCallExpression)
            {
                outerExpr = new AliasExpression(transformed, pe.toString());
            }
            else
            {
                outerExpr = transformed;
            }
            outerProjectionExpressions.add(outerExpr);
        }

        // Remove mixed projection expressions from planProjections (in reverse order to preserve indices)
        if (outerProjectionExpressions != null)
        {
            for (int i = projectionSize - 1; i >= 0; i--)
            {
                if (isMixedProjection(planProjections.get(i)))
                {
                    planProjections.remove(i);
                }
            }
        }

        int sortItemSize = planData.sortItems.size();
        // Analyze sort expressions
        for (int i = 0; i < sortItemSize; i++)
        {
            ctx.nonAggegatedColumnExpressions.clear();
            SortItem si = planData.sortItems.get(i);
            IExpression sie = si.getExpression()
                    .accept(AggregateColumnsCollector.INSTANCE, ctx);
            validateExpressions(plan, ctx, projectionColumns, unWrappedPlanProjections, planProjections, false, "ORDER BY");
            planData.sortItems.set(i, new SortItem(sie, si.getOrder(), si.getNullOrder(), si.getLocation()));
        }

        // Analyze having predicate
        if (planData.filterPredicate != null)
        {
            ctx.nonAggegatedColumnExpressions.clear();
            planData.filterPredicate = planData.filterPredicate.accept(AggregateColumnsCollector.INSTANCE, ctx);
            validateExpressions(plan, ctx, projectionColumns, unWrappedPlanProjections, planProjections, false, "HAVING");
        }

        ILogicalPlan planInput = plan.getInput()
                .accept(this, context);

        // Inject a compute below aggregate with pushdowns
        if (!pushDownExpressions.isEmpty())
        {
            List<IExpression> projections = pushDownExpressions.stream()
                    .map(p -> new AliasExpression(p.getValue(), p.getKey()))
                    .collect(toList());
            // We need all the input besides the compute projections
            projections.add(new AsteriskExpression(null));
            planInput = new Projection(planInput, projections, null);
        }

        ILogicalPlan result = new Aggregate(planInput, plan.getAggregateExpressions(), planProjections, plan.getParentTableSource());

        // Wrap with a Projection above the Aggregate if mixed expressions were found
        if (outerProjectionExpressions != null)
        {
            // If there is a HAVING predicate, place the Filter between the Aggregate and the outer
            // Projection so it can reference internal aggregate aliases (e.g. __exprN)
            if (planData.filterPredicate != null)
            {
                result = new Filter(result, null, planData.filterPredicate);
                planData.filterPredicate = null;
            }
            result = new Projection(result, outerProjectionExpressions, null);
        }

        return result;
    }

    /**
     * Returns true if the given aggregate projection expression is "mixed": it contains at least one aggregate function call but is not itself purely an aggregate function. Such expressions must be
     * split so that aggregate functions reside in the Aggregate operator and the remaining computation is placed in a Projection above it.
     */
    private static boolean isMixedProjection(IAggregateExpression pe)
    {
        if (!(pe instanceof AggregateWrapperExpression awe))
        {
            return false;
        }
        IExpression inner = awe.getExpression();
        if (inner instanceof AliasExpression ae)
        {
            inner = ae.getExpression();
        }
        // Assignment expressions (@var = expr) are a non-standard extension; never split them
        // CSOFF
        if (inner instanceof se.kuseman.payloadbuilder.core.expression.AssignmentExpression)
        // CSON
        {
            return false;
        }
        // A pure aggregate function is never mixed
        if (inner instanceof IFunctionCallExpression fce
                && fce.getFunctionInfo()
                        .getFunctionType()
                        .isAggregate())
        {
            return false;
        }
        return containsAggregateFunctionCall(inner);
    }

    /** Returns true if the expression or any of its descendants is an aggregate function call. */
    private static boolean containsAggregateFunctionCall(IExpression expression)
    {
        if (expression instanceof IFunctionCallExpression fce
                && fce.getFunctionInfo()
                        .getFunctionType()
                        .isAggregate())
        {
            return true;
        }
        for (IExpression child : expression.getChildren())
        {
            if (containsAggregateFunctionCall(child))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns an expression suitable for referencing the given aggregate projection from an outer Projection operator. For aggregate function calls without an alias this will assign a generated alias
     * in {@code planProjections} at {@code index} so that the returned column reference resolves correctly.
     */
    private static IExpression getAggregateProjectionRef(IAggregateExpression pe, int index, List<IAggregateExpression> planProjections, AggregateColumnsCollector.Context ctx)
    {
        // Pure aggregate function calls (FunctionCallExpression implements IAggregateExpression):
        // let AggregateColumnsCollector handle alias assignment via findAggregateProjection
        if (pe instanceof IFunctionCallExpression)
        {
            return pe.accept(AggregateColumnsCollector.INSTANCE, ctx);
        }
        if (pe instanceof AggregateWrapperExpression awe)
        {
            IExpression inner = awe.getExpression();
            // Already aliased (e.g. AliasExpression wrapping the expression)
            if (inner instanceof AliasExpression ae)
            {
                return new UnresolvedColumnExpression(QualifiedName.of(ae.getAliasString()), -1, null);
            }
            // Column reference or other HasAlias (e.g. group-by column reference)
            if (inner instanceof HasAlias ha)
            {
                String alias = ha.getAlias()
                        .getAlias();
                if (!StringUtils.isBlank(alias))
                {
                    return new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null);
                }
            }
            // No usable alias: generate one and update the projection in planProjections
            String genAlias = "__expr" + ctx.context.expressionCounter++;
            String outputAlias = pe.toString();
            planProjections.set(index, new AggregateWrapperExpression(new AliasExpression(inner, genAlias, outputAlias, awe.isInternal()), awe.isSingleValue(), awe.isInternal()));
            return new UnresolvedColumnExpression(QualifiedName.of(genAlias), -1, null);
        }
        throw new IllegalStateException("Cannot create reference for projection: " + pe);
    }

    private boolean shouldReplaceSortItem(IExpression sortItemExpression, IExpression projectionExpression)
    {
        // Literals must be replaced
        if (sortItemExpression instanceof LiteralIntegerExpression)
        {
            return true;
        }
        else if (projectionExpression instanceof UnresolvedColumnExpression)
        {
            return false;
        }

        // If the sort items target column differs from the projected item then we need to replace the sort item
        if (sortItemExpression instanceof HasAlias ha1
                && projectionExpression instanceof HasAlias ha2)
        {
            String alias1 = ha1.getAlias()
                    .getAlias();
            String alias2 = ha2.getAlias()
                    .getAlias();
            if (!isBlank(alias1))
            {
                return !CI.equals(alias1, alias2);
            }
        }

        return true;
    }

    private void validateExpressions(
    //@formatter:off
            Aggregate plan,
            AggregateColumnsCollector.Context ctx,
            Set<String> projectionColumns,
            List<IExpression> unWrappedPlanProjections,
            List<IAggregateExpression> planProjections,
            boolean onlyValidate,
            String clauseDescription)
            //@formatter:on
    {
        // Verify the column expressions and see if they are among the aggregates or contained inside a aggregate function
        for (UnresolvedColumnExpression ce : ctx.nonAggegatedColumnExpressions)
        {
            String columnName = ce.getAlias()
                    .getAlias()
                    .toLowerCase();

            // A expression that points to a projected column then we can continue since
            // this will be resolved later on like an ordinary column from schema
            if (projectionColumns.contains(columnName))
            {
                continue;
            }

            if (!onlyValidate
                    && !unWrappedPlanProjections.contains(ce))
            {
                planProjections.add(new AggregateWrapperExpression(ce, false, true));
            }
        }
    }

    /** Visitor that traverses sub queries and pushes down computations inside those */
    static class UnresolvedSubQueryExpressionVisitor extends ARewriteExpressionVisitor<Ctx>
    {
        static final UnresolvedSubQueryExpressionVisitor INSTANCE = new UnresolvedSubQueryExpressionVisitor();

        @Override
        public IExpression visit(UnresolvedSubQueryExpression expression, Ctx context)
        {
            // Push a new plan data before processing the subquery to don't mess up stuff
            context.planDatas.push(new PlanData());
            ILogicalPlan plan = expression.getInput()
                    .accept(context.visitor, context);
            context.planDatas.pop();
            return new UnresolvedSubQueryExpression(plan, expression.getLocation());
        }
    }

    /** Visitor that collects {@link UnresolvedColumnExpression}'s */
    static class UnresolvedColumnCollector extends AExpressionVisitor<Void, Set<UnresolvedColumnExpression>>
    {
        static final UnresolvedColumnCollector INSTANCE = new UnresolvedColumnCollector();

        @Override
        public Void visit(UnresolvedColumnExpression expression, Set<UnresolvedColumnExpression> context)
        {
            context.add(expression);
            return null;
        }
    }

    /** Visitor that analyzes expressions participating in aggregates. Order by/having */
    static class AggregateColumnsCollector extends ARewriteExpressionVisitor<AggregateColumnsCollector.Context>
    {
        static final AggregateColumnsCollector INSTANCE = new AggregateColumnsCollector();

        static class Context
        {
            final ALogicalPlanOptimizer.Context context;
            final List<IAggregateExpression> planProjections;
            /** List of expressions and it's generated alias to push down below aggregate */
            final List<Pair<String, IExpression>> pushDownExpressions;

            /** List with original projection expressions that was changed due to push down */
            final List<Pair<String, IExpression>> originalProjectionExpressions = new ArrayList<>();

            /** List with found column expressions that are not contained inside an aggregate function */
            List<UnresolvedColumnExpression> nonAggegatedColumnExpressions = new ArrayList<>();

            boolean insideAggregate;

            Context(ALogicalPlanOptimizer.Context context, List<IAggregateExpression> planProjections, List<Pair<String, IExpression>> pushDownExpressions)
            {
                this.context = context;
                this.planProjections = planProjections;
                this.pushDownExpressions = pushDownExpressions;
            }
        }

        @Override
        public IExpression visit(IFunctionCallExpression expression, Context context)
        {
            // This is a nested function call, then we just re-create it as usual, no special handling is needed
            // validation of nested aggregate functions is performed in SchemaResovled prior to this
            if (context.insideAggregate)
            {
                return super.visit(expression, context);
            }

            boolean isAggregate = expression.getFunctionInfo()
                    .getFunctionType()
                    .isAggregate();

            boolean prevInsideAggregate = context.insideAggregate;
            context.insideAggregate = isAggregate;

            IExpression result;
            if (isAggregate)
            {
                // Try to find an existing projection that matches
                IExpression replacement = findAggregateProjection(expression, context);
                if (replacement != null)
                {
                    result = replacement;
                }
                else
                {
                    List<IExpression> arguments = expression.getArguments();
                    // Lambda functions expressions cannot be pushed down
                    if (!(expression.getFunctionInfo() instanceof LambdaFunction))
                    {
                        int size = arguments.size();
                        arguments = new ArrayList<>(size);
                        for (int i = 0; i < size; i++)
                        {
                            IExpression arg = expression.getArguments()
                                    .get(i)
                                    .accept(this, context);
                            if (isComputed(arg))
                            {
                                String alias = "__expr" + context.context.expressionCounter++;
                                // Replace the computed argument with a column expression for a generated alias
                                context.pushDownExpressions.add(Pair.of(alias, arg));
                                arguments.add(new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null));
                            }
                            else
                            {
                                arguments.add(arg);
                            }
                        }
                    }

                    FunctionCallExpression newFunctionCall = new FunctionCallExpression(expression.getCatalogAlias(), expression.getFunctionInfo(), expression.getAggregateMode(), arguments);

                    // Add an internal projection expression for the pushed down computed expression
                    String alias = "__expr" + context.context.expressionCounter++;
                    context.planProjections.add(new AggregateWrapperExpression(new AliasExpression(newFunctionCall, alias), false, true));

                    // Store the original function call along with it's generated alias to reuse if there are other semantic equal expressions
                    // later on
                    context.originalProjectionExpressions.add(Pair.of(alias, expression));

                    // Return an column expression that references the pushed down function
                    result = new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null);
                }
            }
            else
            {
                result = super.visit(expression, context);
            }

            context.insideAggregate = prevInsideAggregate;
            return result;
        }

        @Override
        public IExpression visit(UnresolvedColumnExpression expression, AggregateColumnsCollector.Context context)
        {
            if (!context.insideAggregate)
            {
                context.nonAggegatedColumnExpressions.add(expression);
            }
            return super.visit(expression, context);
        }

        /**
         * Tries to find provided function call among the already existing projections. If found returns a replaced expression other wise null
         */
        private IExpression findAggregateProjection(IFunctionCallExpression expression, Context context)
        {
            // Search original projection expressions that was pushed down and re-use it's generated alias
            int projectionSize = context.originalProjectionExpressions.size();
            for (int i = 0; i < projectionSize; i++)
            {
                Pair<String, IExpression> pair = context.originalProjectionExpressions.get(i);
                if (expression.semanticEquals(pair.getValue()))
                {
                    return new UnresolvedColumnExpression(QualifiedName.of(pair.getKey()), -1, null);
                }
            }

            // Search projection expressions to see if there already is a projected one that we are targeting with
            // this function expression
            projectionSize = context.planProjections.size();
            // boolean match = false;
            for (int j = 0; j < projectionSize; j++)
            {
                IAggregateExpression projectionExpression = context.planProjections.get(j);
                IExpression exp = projectionExpression;
                HasAlias.Alias alias = null;

                if (exp instanceof AggregateWrapperExpression awe)
                {
                    exp = awe.getExpression();
                }
                if (exp instanceof AliasExpression ae)
                {
                    alias = ae.getAlias();
                    exp = ae.getExpression();
                }

                if (expression.semanticEquals(exp))
                {
                    String projectionAlias;
                    // Non aliased projection, generated a unique expression
                    if (alias == null)
                    {
                        projectionAlias = "__expr" + context.context.expressionCounter++;
                        String outputAlias = projectionExpression.toString();

                        // Replace the projection
                        context.planProjections.set(j, new AggregateWrapperExpression(new AliasExpression(exp, projectionAlias, outputAlias, false), false, false));
                    }
                    // Projection is already aliased, return a column expression referencing that
                    else
                    {
                        projectionAlias = alias.getAlias();
                    }
                    return new UnresolvedColumnExpression(QualifiedName.of(projectionAlias), -1, null);
                }
            }

            return null;
        }
    }

    private static boolean isComputed(IExpression expression)
    {
        if (expression instanceof AliasExpression ae)
        {
            expression = ae.getExpression();
        }

        return !(expression instanceof UnresolvedColumnExpression)
                && !(expression instanceof LiteralExpression);
    }
}
