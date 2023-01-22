package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.common.Option;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ALogicalPlanVisitor;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.OverScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;

/**
 * Base class for logical plan optimization rules. This visitor walks the tree and recreates each node if wanted enabling extended classes to apply rules to certain nodes
 */
abstract class ALogicalPlanOptimizer<C extends ALogicalPlanOptimizer.Context> extends ALogicalPlanVisitor<ILogicalPlan, C>
{
    private final ARewriteExpressionVisitor<C> expressionRewriter;

    ALogicalPlanOptimizer(ARewriteExpressionVisitor<C> expressionRewriter)
    {
        this.expressionRewriter = expressionRewriter;
    }

    static class Context
    {
        final IExecutionContext context;
        Map<String, Schema> schemaByTempTable = emptyMap();
        // Counter used to generate unique expression column names for each pushed down sub expression
        int expressionCounter;

        Context(IExecutionContext context)
        {
            this.context = requireNonNull(context, "context");
        }

        QuerySession getSession()
        {
            return (QuerySession) context.getSession();
        }
    }

    /** Optimize input plan */
    abstract ILogicalPlan optimize(Context context, ILogicalPlan plan);

    /** Create context for this optimizer */
    abstract C createContext(IExecutionContext context);

    @Override
    public ILogicalPlan visit(TableScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }

        return plan;
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }

        return plan;
    }

    @Override
    public ILogicalPlan visit(Projection plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Join plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Filter plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Sort plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(SubQuery plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Limit plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(OperatorFunctionScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(ConstantScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(OverScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(MaxRowCountAssert plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Concatenation plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    protected void visit(IExpression expression, C context)
    {
    }

    protected void visit(List<IExpression> expressions, C context)
    {
    }

    protected boolean preVisit(TableScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(TableFunctionScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Projection plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Aggregate plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Join plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Filter plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Sort plan, C context)
    {
        return true;
    }

    protected boolean preVisit(SubQuery plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Limit plan, C context)
    {
        return true;
    }

    protected boolean preVisit(OperatorFunctionScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(ConstantScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(OverScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(MaxRowCountAssert plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Concatenation plan, C context)
    {
        return true;
    }

    protected ILogicalPlan create(TableScan plan, C context)
    {
        List<Option> options = plan.getOptions();

        if (expressionRewriter != null)
        {
            options = options.stream()
                    .map(o -> new Option(o.getOption(), o.getValueExpression()
                            .accept(expressionRewriter, context)))
                    .collect(toList());
        }
        else
        {
            for (Option option : options)
            {
                visit(option.getValueExpression(), context);
            }
        }

        return new TableScan(plan.getTableSchema(), plan.getTableSource(), plan.getProjection(), plan.isTempTable(), options, plan.getToken());
    }

    protected ILogicalPlan create(TableFunctionScan plan, C context)
    {
        List<IExpression> arguments = plan.getArguments();
        List<Option> options = plan.getOptions();

        if (expressionRewriter != null)
        {
            arguments = arguments.stream()
                    .map(e -> e.accept(expressionRewriter, context))
                    .collect(toList());

            options = options.stream()
                    .map(o -> new Option(o.getOption(), o.getValueExpression()
                            .accept(expressionRewriter, context)))
                    .collect(toList());
        }
        else
        {
            visit(arguments, context);
            for (Option option : options)
            {
                visit(option.getValueExpression(), context);
            }
        }
        return new TableFunctionScan(plan.getTableSource(), plan.getSchema(), arguments, options, plan.getToken());
    }

    protected ILogicalPlan create(Projection plan, C context)
    {
        List<IExpression> expressions = plan.getExpressions();

        if (expressionRewriter != null)
        {
            expressions = expressions.stream()
                    .map(e -> e.accept(expressionRewriter, context))
                    .collect(toList());
        }
        else
        {
            // Visit expressions before plan
            visit(plan.getExpressions(), context);
        }
        return new Projection(plan.getInput()
                .accept(this, context), expressions, plan.isAppendInputColumns());
    }

    protected ILogicalPlan create(Aggregate plan, C context)
    {
        List<IExpression> aggregateExpressions = plan.getAggregateExpressions();
        List<IAggregateExpression> projectionExpressions = plan.getProjectionExpressions();
        if (expressionRewriter != null)
        {
            aggregateExpressions = aggregateExpressions.stream()
                    .map(e -> e.accept(expressionRewriter, context))
                    .collect(toList());

            projectionExpressions = projectionExpressions.stream()
                    .map(e -> (IAggregateExpression) e.accept(expressionRewriter, context))
                    .collect(toList());
        }
        else
        {
            visit(aggregateExpressions, context);
            for (IAggregateExpression projectionExpression : projectionExpressions)
            {
                visit(projectionExpression, context);
            }
        }

        return new Aggregate(plan.getInput()
                .accept(this, context), aggregateExpressions, projectionExpressions);
    }

    protected ILogicalPlan create(Join plan, C context)
    {
        IExpression condition = plan.getCondition();
        if (condition != null)
        {
            if (expressionRewriter != null)
            {
                condition = condition.accept(expressionRewriter, context);
            }
            else
            {
                visit(condition, context);
            }
        }
        return new Join(plan.getOuter()
                .accept(this, context),
                plan.getInner()
                        .accept(this, context),
                plan.getType(), plan.getPopulateAlias(), condition, plan.getOuterReferences(), plan.isSwitchedInputs());
    }

    protected ILogicalPlan create(Sort plan, C context)
    {
        List<SortItem> sortItems = plan.getSortItems()
                .stream()
                .map(si ->
                {
                    IExpression expression = si.getExpression();
                    if (expressionRewriter != null)
                    {
                        expression = expression.accept(expressionRewriter, context);
                    }
                    else
                    {
                        visit(expression, context);
                    }
                    return new SortItem(expression, si.getOrder(), si.getNullOrder(), si.getToken());
                })
                .collect(toList());

        return new Sort(plan.getInput()
                .accept(this, context), sortItems);
    }

    protected ILogicalPlan create(Filter plan, C context)
    {
        IExpression predicate = plan.getPredicate();
        if (expressionRewriter != null)
        {
            predicate = predicate.accept(expressionRewriter, context);
        }
        else
        {
            visit(predicate, context);
        }
        return new Filter(plan.getInput()
                .accept(this, context), plan.getTableSource(), predicate);
    }

    protected ILogicalPlan create(SubQuery plan, C context)
    {
        return new SubQuery(plan.getInput()
                .accept(this, context), plan.getAlias(), plan.getToken());
    }

    protected ILogicalPlan create(Limit plan, C context)
    {
        IExpression limitExpression = plan.getLimitExpression();
        if (expressionRewriter != null)
        {
            limitExpression = limitExpression.accept(expressionRewriter, context);
        }
        else
        {
            visit(limitExpression, context);
        }
        return new Limit(plan.getInput()
                .accept(this, context), limitExpression);
    }

    protected ILogicalPlan create(OperatorFunctionScan plan, C context)
    {
        return new OperatorFunctionScan(plan.getSchema(), plan.getInput()
                .accept(this, context), plan.getCatalogAlias(), plan.getFunction(), plan.getToken());
    }

    protected ILogicalPlan create(ConstantScan plan, C context)
    {
        return plan;
    }

    protected ILogicalPlan create(OverScan plan, C context)
    {
        return plan;
    }

    protected ILogicalPlan create(MaxRowCountAssert plan, C context)
    {
        return new MaxRowCountAssert(plan.getInput()
                .accept(this, context), plan.getMaxRowCount());
    }

    protected ILogicalPlan create(Concatenation plan, C context)
    {
        return new Concatenation(plan.getChildren()
                .stream()
                .map(p -> p.accept(this, context))
                .collect(toList()));
    }

    /** Extract columns from provided expressions */
    protected static Set<ColumnReference> collectColumns(List<IExpression> expressions)
    {
        Set<ColumnReference> columns = new HashSet<>();
        for (IExpression expression : expressions)
        {
            expression.accept(ColumnReferenceExtractor.INSTANCE, columns);
        }
        return columns;
    }

    /** Visitor that collects {@link ColumnReference}'s from an expression */
    static class ColumnReferenceExtractor extends AExpressionVisitor<Void, Set<ColumnReference>>
    {
        static final ColumnReferenceExtractor INSTANCE = new ColumnReferenceExtractor();

        @Override
        public Void visit(UnresolvedColumnExpression expression, Set<ColumnReference> columns)
        {
            throw new IllegalArgumentException("Unresolved column expression should not be visited");
        }

        @Override
        public Void visit(IColumnExpression expression, Set<ColumnReference> context)
        {
            if (expression.getColumnReference() != null)
            {
                context.add(expression.getColumnReference());
            }
            return null;
        }
    }
}
