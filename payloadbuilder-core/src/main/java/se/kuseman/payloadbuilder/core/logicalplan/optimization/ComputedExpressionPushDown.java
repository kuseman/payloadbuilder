package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

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
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.LambdaFunction;
import se.kuseman.payloadbuilder.core.common.SortItem;
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
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
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
        return new Ctx(context);
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
    public ILogicalPlan visit(Projection plan, Ctx context)
    {
        PlanData planData = context.planDatas.peek();

        // Nothing to rewrite
        if (planData.sortItems.isEmpty())
        {
            return plan;
        }

        Map<String, Integer> projectionExpressionByName = new HashMap<>();

        List<IExpression> projectionExpressions = plan.getExpressions();
        int size = projectionExpressions.size();
        int firstAsteriskIndex = -1;
        for (int i = 0; i < size; i++)
        {
            IExpression e = projectionExpressions.get(i);
            if (e instanceof AsteriskExpression
                    && firstAsteriskIndex == -1)
            {
                // One based since order by ordinal is one based
                firstAsteriskIndex = i + 1;
            }

            // Index all named projections, this to be able to find out if we need to add a projection that is sorted on and is not projected
            if (e instanceof HasAlias)
            {
                HasAlias.Alias alias = ((HasAlias) e).getAlias();
                projectionExpressionByName.put(alias.getAlias()
                        .toLowerCase(), i);
            }
        }

        List<Pair<String, Integer>> pushDownProjections = new ArrayList<>();

        List<IExpression> newProjectionExpressions = new ArrayList<>(plan.getExpressions());

        // Search sort items for rewrite
        // - Ordinal sorts (ORDER BY 1)
        // - Sorts on columns not projected => add to projection as internal
        int sortItemSize = planData.sortItems.size();
        for (int j = 0; j < sortItemSize; j++)
        {
            SortItem item = planData.sortItems.get(j);

            int projectionExpressionIndex = -1;
            if (item.getExpression() instanceof LiteralIntegerExpression)
            {
                int index = ((LiteralIntegerExpression) item.getExpression()).getValue();

                if (index <= 0
                        || (firstAsteriskIndex == -1
                                && index > projectionExpressions.size()))
                {
                    throw new QueryException("ORDER BY position is out of range");
                }
                // There are asterisks before or on the index then we sort by the ordinal runtime
                else if (firstAsteriskIndex >= 0
                        && index >= firstAsteriskIndex)
                {
                    continue;
                }

                projectionExpressionIndex = index - 1;
            }
            else if (item.getExpression() instanceof UnresolvedColumnExpression)
            {
                UnresolvedColumnExpression colExpression = (UnresolvedColumnExpression) item.getExpression();

                /*
                 * select col from table order by col2 <-- col2 is not projected need to add it to projections as internal
                 */

                String column = colExpression.getAlias()
                        .getAlias();

                Integer projectionIndex = projectionExpressionByName.get(column.toLowerCase());

                // Add an interal projection for missing column
                if (projectionIndex == null)
                {
                    newProjectionExpressions.add(new AliasExpression(colExpression, column, true));
                    continue;
                }

                // Verify constant sort
                if (projectionExpressions.get(projectionIndex)
                        .isConstant())
                {
                    throw new ParseException("Order by constant encountered", item.getToken());
                }

                continue;
            }

            if (projectionExpressionIndex == -1)
            {
                /*@formatter:off
               * Find semantic equivalent expression in sort items as in projection ie:
               * select col1 + col2
               * from table 
               * order by col1 + col2
               * @formatter:on
               */

                for (int i = 0; i < size; i++)
                {
                    if (item.getExpression()
                            .semanticEquals(projectionExpressions.get(i)))
                    {
                        projectionExpressionIndex = i;
                        break;
                    }
                }
            }

            if (projectionExpressionIndex >= 0)
            {
                IExpression projectionExpression = projectionExpressions.get(projectionExpressionIndex);

                // Sorting on a constant => error
                if (projectionExpression.isConstant())
                {
                    throw new ParseException("Order by constant encountered", item.getToken());
                }

                // Projected expression has an alias, point the sort item to the alias
                if (projectionExpression instanceof HasAlias)
                {
                    String alias = ((HasAlias) projectionExpression).getAlias()
                            .getAlias();
                    // Point the sort item to the projected alias
                    planData.sortItems.set(j, new SortItem(new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null), item.getOrder(), item.getNullOrder(), item.getToken()));
                }
                // Computed projection expression, push down
                else if (isComputed(projectionExpression))
                {
                    String alias = "__expr" + context.expressionCounter++;
                    pushDownProjections.add(Pair.of(alias, projectionExpressionIndex));
                    planData.sortItems.set(j, new SortItem(new UnresolvedColumnExpression(QualifiedName.of(alias), -1, null), item.getOrder(), item.getNullOrder(), item.getToken()));
                }
            }
        }

        ILogicalPlan input = plan.getInput()
                .accept(this, context);
        //
        // If no projections was pushed down return original projection with visited input
        if (pushDownProjections.isEmpty())
        {
            return new Projection(input, newProjectionExpressions, plan.isAppendInputColumns());
        }

        List<IExpression> computedExpressions = new ArrayList<>(pushDownProjections.size());

        // Re-create projections with the pushed down expressions and their new aliases
        for (Pair<String, Integer> p : pushDownProjections)
        {
            IExpression originalExpression = projectionExpressions.get(p.getRight());
            String name = p.getLeft();
            String outputName = originalExpression.toString();
            computedExpressions.add(new AliasExpression(originalExpression, p.getLeft(), true));

            // Add the expression with the generated alias name
            newProjectionExpressions.set(p.getRight(), new AliasExpression(new UnresolvedColumnExpression(QualifiedName.of(p.getLeft()), -1, null), name, outputName, false));
        }
        // Insert a compute operator before projection
        input = new Projection(input, computedExpressions, true);
        return new Projection(input, newProjectionExpressions, plan.isAppendInputColumns());
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

            return new Filter(input, plan.getTableSource(), filterPredicate);
        }

        return super.create(plan, context);
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
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
        UnresolvedColumnCollector.Context ctx = new UnresolvedColumnCollector.Context(context, planProjections, pushDownExpressions);

        int sortItemSize = planData.sortItems.size();
        // Analyze sort expressions
        for (int i = 0; i < sortItemSize; i++)
        {
            ctx.nonAggegatedColumnExpressions.clear();
            SortItem si = planData.sortItems.get(i);
            IExpression sie = si.getExpression()
                    .accept(UnresolvedColumnCollector.INSTANCE, ctx);
            validateExpressions(plan, ctx, projectionColumns, unWrappedPlanProjections, planProjections, false, "ORDER BY");
            planData.sortItems.set(i, new SortItem(sie, si.getOrder(), si.getNullOrder(), si.getToken()));
        }

        // Analyze having predicate
        if (planData.filterPredicate != null)
        {
            ctx.nonAggegatedColumnExpressions.clear();
            planData.filterPredicate = planData.filterPredicate.accept(UnresolvedColumnCollector.INSTANCE, ctx);
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
            planInput = new Projection(planInput, projections, true);
        }

        return new Aggregate(planInput, plan.getAggregateExpressions(), planProjections);
    }

    private void validateExpressions(
    //@formatter:off
            Aggregate plan,
            UnresolvedColumnCollector.Context ctx,
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

    /** Visitor that analyzes expressions participating in aggregates. Order by/having */
    static class UnresolvedColumnCollector extends ARewriteExpressionVisitor<UnresolvedColumnCollector.Context>
    {
        static final UnresolvedColumnCollector INSTANCE = new UnresolvedColumnCollector();

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
        public IExpression visit(UnresolvedColumnExpression expression, UnresolvedColumnCollector.Context context)
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

                if (exp instanceof AggregateWrapperExpression)
                {
                    exp = ((AggregateWrapperExpression) exp).getExpression();
                }
                if (exp instanceof AliasExpression)
                {
                    alias = ((AliasExpression) exp).getAlias();
                    exp = ((AliasExpression) exp).getExpression();
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
        if (expression instanceof AliasExpression)
        {
            expression = ((AliasExpression) expression).getExpression();
        }

        return !(expression instanceof UnresolvedColumnExpression)
                && !(expression instanceof LiteralExpression);
    }
}
